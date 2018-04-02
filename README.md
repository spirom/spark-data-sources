
# Spark External Data Sources using the V2 API

This project illustrates the new V2 Apache Spark External Data Source API as
introduced in Spark 2.3.0.

It consists of:
* A simplistic in-memory database system (ExampleDB) that supports all of the data access
paradigms needed to illustrate the features of the API
* A series of data sources of varying complexity, all implemented in Java since
native Java support is a major goal of the new API (Scala versions mey be added in the future)
* A series of Spark examples to exercise the various data sources implemented
(also in Java for now)

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

# Target Audience

This project is targeted at developers who are designing and implementing a new Spark
external data source for some data store with which they are reasonably familiar, and
need information on Spark's new (V2) model for integrating external data sources.

# Designing ExampleDB

This project is rather unusual in the decision to develop, from scratch, a simple
in-memory database system (ExampleDB) for the sole purpose of providing a simple
example integration point
for a wide range of Spark external data source examples. It's important to
understand the goals and (especially) non-goals of ExampleDB.

## Goals

* All communication with ExampleDB should be via remote procedure calls (RPCs) so it
is clearly
separated from the data sources that use it.
* For simplicity, the example Spark code should have the option of running ExampleDB
in-process on
the driver node (to avoid cluttering the examples with process control issues).
* ExampleDB should provide a suitable data model and set of API entry points to
support ALL of the features illustrated in the example data sources. Of course, this is
made a rather tough goal by the remarkable breadth of the new data source API, spanning
both row and column based data representations as well as batch and streaming queries.

## Non-goals

* The data model and API of ExampleDB don't really have to make sense as a whole --
it's sufficient if they merely mimic features in the real world with sufficient fidelity
that developers can see how to map them to the features of the data store they are trying
to integrate with Spark.
* The implementation of ExampleDB doesn't have to make sense. Achieving high performance is not
a goal and as such neither is the use of advanced DBMS implementation techniques. Since
ExampleDB only has to serve these examples, it is implemented with expediency as its major focus.

# The Data Sources

These can be found under [src/main/java/datasources](src/main/java/datasources).

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>

<tr>
<td><a href="src/main/java/datasources/SimpleRowDataSource.java">SimpleRowDataSource.java</a></td>
<td>
<p>An extremely simple DataSource that supports sequential reads (i.e.: on just one executor)
from ExampleDB. It only supports reads from a single, pre-defined table with a
pre-defined schema. This DataSource is probably about a simple as one that reads from a
remote database can get.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/datasources/FlexibleRowDataSource.java">FlexibleRowDataSource.java</a></td>
<td>
<p>Another simple DataSource that supports sequential reads (i.e.: on just one executor)
from ExampleDB. It gets a table name from its configuration and infers a schema from
that table.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/datasources/ParallelRowDataSource.java">ParallelRowDataSource.java</a></td>
<td>
<p>Another simple DataSource that supports parallel reads (i.e.: on multiple executors)
from ExampleDB. It gets a table name from its configuration and infers a schema from
that table. If a number of partitions is specified in properties, it is used. Otherwise,
the table's default partition count (always 4 in ExampleDB) is used.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/datasources/PartitioningRowDataSource.java">PartitioningRowDataSource.java</a></td>
<td>
<p>This also supports parallel reads (i.e.: on multiple executors)
from the ExampleDB.
The interesting feature of this example is that it supports informing the
Spark SQL optimizer whether the table is partitioned in the right way to avoid shuffles
in certain queries. One example is grouping queries, where shuffles can be avoided if the
table is clustered in such a way that each group (cluster) is fully contained in a
single partition. Since ExampleDB only supports clustered indexes on single columns,
in practice a shuffle can be avoided if the table is clustered on one of the grouping
(In ExampleDB clustered tables, splits always respect clustering.)
</p>
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
<p>Instead uses the FlexibleRowDataSource to infer the schema of a specified table
and query it, again sequentially, again resulting in a Dataset with a single partition.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/examples/ReadParallel.java">ReadParallel.java</a></td>
<td>
<p>Uses the ParallelRowDataSource to infer the schema of a specified table
and query it, this time in parallel, resulting in Datasets with multiple partitions.
The example shows both taking the default number of partitions and
specifying a partition count.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/examples/ReadPartitionAware.java">ReadPartitionAware.java</a></td>
<td>
<p>Uses the PartitioningRowDataSource to avoid a shuffle in a grouping/aggregation query
against a table that is clustered ont he grouping column. It achieves this by using the
SupportsReportPartitioning mixin for the DataSourceReader interface.</p>
</td>
</tr>
<tr>
<td><a href="src/main/java/examples/ReadPartitionAware_Mismatch.java">ReadPartitionAware_Mismatch.java</a></td>
<td>
<p>This uses the same data source as the previous example but doesn't cluster the table, thus
illustrating the shuffle that takes place. .</p>
</td>
</tr>
</table>

# Logging

Consider adjusting the log levels in
[src/main/resources/log4j.properties](src/main/resources/log4j.properties)
to adjust verbosity as needed.

Notice that the data sources and the ExampleDB components both have entries there.


