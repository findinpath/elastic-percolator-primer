package org.elasticsearch.percolator;


import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class is based on the body of the class
 * <a href="https://github.com/elastic/elasticsearch/blob/master/modules/percolator/src/test/java/org/elasticsearch/percolator/CandidateQueryTests.java>CandidateQueryTests.java</a>
 * present in the <a href="https://github.com/elastic/elasticsearch">Elasticsearch Github repository</a>.
 *
 * The class provides a series of simple tests to easily explain the percolator functionality.
 *
 * The Elasticsearch percolation works in two phases:
 * <ul>
 *     <li>first, find out the queries which could come in question for being matched against the memory index</li>
 *     <li>second, take the selected queries and run them against the memory index</li>
 * </ul>
 *
 * Therefor it is very important that in the first phase to filter out as much as possible for the queries that don't
 * fit the percolated document so that a reduced number of queries is executed against the memory index.
 */
public class CandidateQueryTests extends ESSingleNodeTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private DocumentMapper documentMapper;
    private DirectoryReader directoryReader;
    private IndexService indexService;
    private MapperService mapperService;

    private PercolatorFieldMapper fieldMapper;
    private PercolatorFieldMapper.FieldType fieldType;

    private List<Query> queries;
    private PercolateQuery.QueryStore queryStore;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Before
    public void init() throws Exception {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        indexWriter = new IndexWriter(directory, config);

        String indexName = "test";
        indexService = createIndex(indexName, Settings.EMPTY);
        mapperService = indexService.mapperService();

        String mapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int_field").field("type", "integer").endObject()
                .startObject("long_field").field("type", "long").endObject()
                .startObject("half_float_field").field("type", "half_float").endObject()
                .startObject("float_field").field("type", "float").endObject()
                .startObject("double_field").field("type", "double").endObject()
                .startObject("ip_field").field("type", "ip").endObject()
                .startObject("field").field("type", "keyword").endObject()
                .endObject().endObject().endObject());
        documentMapper = mapperService.merge("type", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE);

        String queryField = "query_field";
        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject(queryField).field("type", "percolator").endObject().endObject()
                .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE);
        fieldMapper = (PercolatorFieldMapper) mapperService.documentMapper().mappers().getMapper(queryField);
        fieldType = (PercolatorFieldMapper.FieldType) fieldMapper.fieldType();

        queries = new ArrayList<>();
        queryStore = ctx -> docId -> this.queries.get(docId);
    }

    @After
    public void deinit() throws Exception {
        directoryReader.close();
        directory.close();
    }


    /**
     * This test showcases the scenario of for documents having an integer field
     * having a value within a specified range.
     *
     * For all the percolate queries that deal with range queries on the field,
     * there will be built a k-dimensional tree and when doing the percolation
     * for a specific document, there will be filtered only the query documents
     * that intersect the integer field of the document.
     *
     * In this fashion, instead of checking whether all the queries match the submitted
     * document, only the queries having the `int_field` field range (e.g. : 0 TO 5, 1 TO 10) intersecting with the
     * `int_field` (e.g. : 3) of the percolated document will be selected for being executed
     * against the memory index.
     *
     *
     * @see <a href="https://www.youtube.com/watch?v=Z4dNLvno-EY">KD-Trees and Range search</a>
     */
    public void testIntRangeQueries() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        addQuery(IntPoint.newRangeQuery("int_field", 0, 5), docs);
        addQuery(IntPoint.newRangeQuery("int_field", 10, 20), docs);
        addQuery(IntPoint.newRangeQuery("int_field", 1, 10), docs);
        addQuery(IntPoint.newRangeQuery("int_field", 20, 40), docs);
        addQuery(IntPoint.newRangeQuery("int_field", 30, 40), docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = VersionUtils.randomIndexCompatibleVersion(random());
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(
                Collections.singleton(new IntPoint("int_field", 3)),
                new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query query = fieldType.percolateQuery("_name", queryStore,
                Collections.singletonList(new BytesArray("{}")),
                percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 1);
        assertEquals(2L, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);
    }

    /**
     * One of the most used kind of percolation queries is the term query.
     *
     * When trying to percolate for documents having a specific term, out of
     * `greeting` `TermQuery` field will be built internally a Lucene inverted index.
     *
     * The inverted index for the `greeting` field will be then used to search for matching terms
     * corresponding to the tokens extracted from the percolated document:
     *
     * <ul>
     *     <li>`query_field.extracted_terms:greeting happy`</li>
     *     <li>`query_field.extracted_terms:greeting holidays`</li>
     * </ul>
     *
     * In this fashion, only the queries that contain a term corresponding to one of the tokens
     * of the greeting will be selected to be executed against the memory index.
     *
     *
     * @see org.apache.lucene.search.TermScorer
     */
    public void testTermQueries() throws Exception {
        List<ParseContext.Document> documents = new ArrayList<>();
        addQuery(new TermQuery(new Term("greeting", "happy")), documents);
        addQuery(new TermQuery(new Term("greeting", "day")), documents);
        addQuery(new TermQuery(new Term("greeting", "good")), documents);
        addQuery(new TermQuery(new Term("greeting", "hi")), documents);
        addQuery(new TermQuery(new Term("greeting", "bye")), documents);

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        MemoryIndex memoryIndex =MemoryIndex.fromDocument(
                Collections.singleton(new TextField("greeting", "happy holidays", Field.Store.NO)),
                new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        Query percolateQuery = fieldType.percolateQuery("_name", queryStore,
                Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, Version.CURRENT);
        TopDocs topDocs = shardSearcher.search(percolateQuery, 10);

        assertEquals(1L, topDocs.totalHits.value);
    }


    /**
     * At the time of this writing, the Geo queries are not supported for extraction
     * in the `org.elasticsearch.percolator.PercolatorFieldMapper`.
     * This is why this kind of query will need to be executed directly against the
     * memory index containing the document to percolate.
     *
     * @see PercolatorFieldMapper#EXTRACTION_FAILED
     */
    public void testLatLonQueries() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();

        double COLOMBO_LAT = 6.927079, COLOMBO_LON = 79.861244;

        //distance queries are not yet supported for percolation
        Query distanceQuery = LatLonPoint.newDistanceQuery("location", COLOMBO_LAT,COLOMBO_LON, 30000);
        addQuery(distanceQuery, docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = VersionUtils.randomIndexCompatibleVersion(random());

        Document document = new Document();
        document.add(new LatLonPoint("location", 6.821994,79.886208));


        MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")),
                percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 10);

        assertEquals(1L, topDocs.totalHits.value);
    }


    private void addQuery(Query query, List<ParseContext.Document> docs) {
        IndexMetadata build = IndexMetadata.builder("")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(0).build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(settings,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(query, parseContext);
        ParseContext.Document queryDocument = parseContext.doc();
        // Add to string representation of the query to make debugging easier:
        queryDocument.add(new StoredField("query_to_string", query.toString()));
        docs.add(queryDocument);
        queries.add(query);
    }
}