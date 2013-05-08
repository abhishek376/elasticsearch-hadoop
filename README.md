# ElasticSearch Hive
Read and write data to/from ElasticSearch within Hive libraries. This project is fork from https://github.com/elasticsearch/elasticsearch-hadoop. 

This project includes support for

1) Not only index documents but also delete documents from ES.  

2) Example on how to insert nested documents from Hive to ES.

# Installation
Its very easy to build the project using gradle. [build](#building-the-source) the project yourself. 
ElasticSearch Hadoop uses Gradle for its build system and it is not required to have it installed on your machine.
To create a distributable jar, run `gradlew -x test build` from the command line; once completed you will find the jar in `build\libs`.

# Usage

## Configuration Properties

All configuration properties start with `es` prefix. Note that the `es.internal` namespace is reserved for the library internal use and should _not_ be used by the user at any point.

The properties are read mainly from the Hadoop configuration but the user can specify (some of) them directly depending on the library used (see the relevant documentation below). The full list is available [here](http://github.com/elasticsearch/elasticsearch-hadoop/tree/master/src/main/java/org/elasticsearch/hadoop/cfg/ConfigurationOptions.java).

### Required
```
es.resource=<ES resource location, relative to the host/port specified above. Can be an index or a query>
```
### Optional
```
es.host=<ES host address> 				       # defaults to localhost
es.port=<ES REST port>    				       # defaults to 9200
es.bulk.size.bytes=<bulk size in bytes>        # defaults to 10mb
es.bulk.size.entries=<bulk size in entries>    # defaults to 0 (meaning it's not set)
es.http.timeout=<timeout for http connections> # defaults to 1m
es.operation.type=<operation type>             # options are index/delete defaults to index

```
```
## [Hive][]
ES-Hadoop provides a Hive storage handler for ElasticSearch, meaning one can define an [external table][] on top of ES.

Add es-hadoop-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/es-hadoop-<version>.jar;
```
### Reading
To read data from ES, define a table backed by the desired index:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT, 
    name    STRING
   STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists/_search?q=me*');
```
The fields defined in the table are mapped to the JSON when communicating with ElasticSearch. Notice the use of `TBLPROPERTIES` to define the location, that is the query used for reading from this table:
```
SELECT * FROM artists;
```

### Writing
To write data, a similar definition is used but with a different `es.resource`:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT, // _id field
    name    STRING
   STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists/');
```
FOR DELETE
```SQL 
CREATE EXTERNAL TABLE delete_artists (
    id      BIGINT, // _id field
    name    STRING
   STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists/', 'es.operation.type' = 'delete');
```

Any data passed to the table is then passed down to ElasticSearch; for example considering a table `s`, mapped to a TSV/CSV file, one can index it to ElasticSearch like this:

```SQL
INSERT OVERWRITE TABLE artists 
    SELECT id, s.name FROM source s;
```

As one can note, currently the reading and writing are treated separately.

### How delete works

Elastic search bulk delete needs the _id to delete. By default ES assing some value to _id. So you need to specify  which column we want to treat at _id while indexing. We will use the same 
column while deleting. 

Create an external table with operation type as delete and

```SQL
INSERT OVERWRITE TABLE delete_artists 
    SELECT id FROM source s;
```

Any data passed to the table is then passed down to ElasticSearch to delete the document.

You can reach me at @abhishek376 on twitter.





