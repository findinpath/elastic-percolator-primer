Elasticsearch Percolator Primer
===============================


This simple project is more like a note to self to gather an understanding
on how Elasticsearch Percolator functionality works under the hood.


The _Elasticsearch Percolator_ functionlity can be found under the
[percolator]((https://github.com/elastic/elasticsearch/tree/master/modules/percolator))
module.

The basic idea behind the percolator is that (as denoted by the documentation
of the [percolate](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-percolate-query.html)
query):
 
> It can be used to match queries stored in an index.
> The percolate query itself contains the document 
> that will be used as query to match with the stored queries.


Instead of naively executing all the queries stored in the index against
the percolated document, Elasticsearch will filter out the queries that don't
match the document by doing a search  on top of the stored queries.
Only the queries that are matching the document (or the ones that can't be evaluated in bulk)
will be finally executed against a memory index that contains only the document to percolate.

The _percolate_ query is called _turning search upside down_ in the [Elasticsearch in action](https://livebook.manning.com/book/elasticsearch-in-action/appendix-e/)
book for the following reasons:

- You index queries instead of documents. This registers the query in memory, so it can be quickly run later.
- You send a document to Elasticsearch instead of a query. This is called percolating a document, basically indexing it into a small, in-memory index. Registered queries are run against the small index, so Elasticsearch finds out which queries match.
- You get back a list of queries matching the document, instead of the other way around like a regular search.
 



The percolator analyses the queries and creates appropriate data structures in order to
be able to filter only the relevant search queries that need to be executed on the memory
index containing the percolated document: 

- for a  _term_ (e.g. : search for all the documents containing `elastic` ) query there will be used a `org.apache.lucene.search.TermScorer`
that will make use of the Lucene's inverted index functionality for retrieving the documents that contain the searched term
- for a int field (e.g. : search all the real estate object with `4` rooms) an intersection will be made
between the range of the percolated document (e.g. : `4` to `4`) and the [KD tree](https://www.youtube.com/watch?v=Z4dNLvno-EY) 
containing the ranges (e.g. : `0` TO `3` , `4` TO `*`, `2` TO `5`, etc.)

  
 
This very simple project contains a series of tests in the [CandidateQueryTests.java](src/test/java/org/elasticsearch/percolator/CandidateQueryTests.java)
test class that showcase how the percolator actually works behind the scenes.
 