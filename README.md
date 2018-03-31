
# Spark External Data Sources with the V2 API

This project illustrates the new V2 Apache Spark External Data Source API as
introduced in Spark 2.3.0.

I consists of:
* A simplistic in-memory database system (ExampleDB) that supports all of the data access
paradigms needed to illustrate the features of the API
* A series of data sources of varying complexity, all implemented in Java since
native Java support is a major goal of the new API (Scala versions mey be added in the future)
* A series of Spark examples to exercise the various data sources implemented
(also in Java for the time being.)

The project has spun out of the following older projects:
* [https://github.com/spirom/LearningSpark](https://github.com/spirom/LearningSpark) -- a wide range of Spark examples in Scala
* [https://github.com/spirom/learning-spark-with-java](https://github.com/spirom/learning-spark-with-java) -- a small set of Spark examples in Java
* [https://github.com/spirom/spark-streaming-with-kafka](https://github.com/spirom/spark-streaming-with-kafka) -- a range of Spark Streaming examples with embedded Kafka and Zookeeper instances

# Goals and Assumptions

* Use a "remote" database to illustrate everything
    * Forces us to address connection management in the design of the data sources
* Use a single database system for all the kinds of sources for simplicity
    * Supports transactions and splits to address resiliency and parallelism
* all the different kinds of data sources
    * different computation models
    * different storage models (rows and columns)
    * different levels of optimization support
* Show the different ways to use the data sources from Spark

# Designing ExampleDB



# The Data Sources

These can be found under [src/main/java/datasources](src/main/java/datasources).

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>

<tr>
<td><a href="src/main/java/datasources/SimpleRowDataSource.java">SimpleRowDataSource.java</a></td>
<td>
<p>An extremely simple DataSource that supports sequential reads (i.e.: on just one executor)
from the ExampleDB. It only supports reads from a single, pre-defined table with a
pre-defined schema. This DataSource is probably about a simple as one that reads from a
remote database can get.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/datasources/FlexibleRowDataSource.java">FlexibleRowDataSource.java</a></td>
<td>
<p>Another simple DataSource that supports sequential reads (i.e.: on just one executor)
from the ExampleDB. It gets a table name from its configuration and infers a schema from
that table.</p>
</td>
</tr>
</table>

# The Spark Examples

These can be found under [src/main/java/examples](src/main/java/examples).

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>

<tr>
<td><a href="src/main/java/examples/Basic.java">Basic.java</a></td>
<td>
<p>Simplest example that uses direct ExampleDB calls to populate a table and then
uses the SimpleRowDataSource to query it from Spark. Since that data source is
sequential the resulting Dataset has just one partition.
Since the data source reads from a single, hard coded table with a hard coded schema,
the table name is not specified int he Spark code.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/examples/ReadNamedTable.java">ReadNamedTable.java</a></td>
<td>
<p>Instead uses the FlexibleRowDataStore to infer the schema of a specified table
and query it, again sequentially, again resulting in a Dataset with a single partition.</p>
</td>
</tr>
</table>

# Logging

Consider adjusting the log levels in
[src/main/resources/log4j.properties](src/main/resources/log4j.properties)
to adjust verbosity as needed.

Notice that the data sources and the ExampleDB components both have entries there.


