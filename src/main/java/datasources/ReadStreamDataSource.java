package datasources;

import datasources.utils.DBClientWrapper;
import datasources.utils.DBTableReader;
import edb.common.SimpleSplit;
import edb.common.Split;
import edb.common.UnknownTableException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Another simple DataSource that supports sequential reads (i.e.: on just one executor)
 * from the ExampleDB. It gets a table name from its configuration and infers a schema from
 * that table.
 */
public class ReadStreamDataSource implements DataSourceV2, MicroBatchReadSupport {

    static Logger log = Logger.getLogger(ReadStreamDataSource.class.getName());

    /**
     * Spark calls this to create the reader. Notice how it pulls the host and port
     * on which ExampleDB is listening, as well as a table name, from the supplied options.
     * @param schema
     * @param checkpointLocation
     * @param options
     * @return
     */
    @Override
    public MicroBatchReader createMicroBatchReader(
            Optional<StructType> schema,
            String checkpointLocation,
            DataSourceOptions options) {
        String host = options.get("host").orElse("localhost");
        int port = options.getInt("port", -1);
        String table = options.get("table").orElse("unknownTable"); // TODO: throw
        return new Reader(host, port, table);
    }


    static class ExampleDBOffset extends Offset {

        public ExampleDBOffset() {
            _offset = 0;
        }

        public ExampleDBOffset(long offset) {
            _offset = offset;
        }

        @Override
        public String json() {
            return "{\"example-db-offset\":" + _offset + "}";
        }

        public long getOffset() { return _offset; }

        private long _offset;

    }

    static class ExampleDBOffsetRange {
        ExampleDBOffsetRange(ExampleDBOffset start, ExampleDBOffset end) {
            _start = start;
            _end = _end;
        }

        public ExampleDBOffset getStart() { return _start; }

        public ExampleDBOffset getEnd() { return _end; }

        private ExampleDBOffset _start;
        private ExampleDBOffset _end;
    }

    /**
     * This is how Spark discovers the source table's schema by requesting a schema from ExmapleDB,
     * and how it obtains the reader factories to be used by the executors to create readers.
     * In this case only one reader factory is created, supporting just one executor, so the
     * resulting Dataset will have only a single partition -- that's why this DataSource
     * only provides sequential reads.
     */
    static class Reader implements MicroBatchReader {

        static Logger log = Logger.getLogger(Reader.class.getName());

        public Reader(String host, int port, String table) {
            _host = host;
            _port = port;
            _table = table;
            _startOffset = new ExampleDBOffset();
            _endOffset = new ExampleDBOffset();
        }

        private StructType _schema;
        private String _host;
        private int _port;
        private String _table;
        private ExampleDBOffset _startOffset;
        private ExampleDBOffset _endOffset;

        @Override
        public StructType readSchema() {
            log.info("reading schema");
            if (_schema == null) {
                log.info("really reading schema");
                DBClientWrapper db = new DBClientWrapper(_host, _port);
                db.connect();
                try {
                    _schema = db.getSparkSchema(_table);
                } catch (UnknownTableException ute) {
                    throw new RuntimeException(ute);
                } finally {
                    db.disconnect();
                }
            }
            return _schema;
        }

        @Override
        public List<DataReaderFactory<Row>> createDataReaderFactories() {
            log.info("creating a single factory");
            return java.util.Collections.singletonList(
                    new SimpleDataReaderFactory(_host, _port, _table, readSchema(),
                            new ExampleDBOffsetRange(_startOffset, _endOffset)));
        }

        @Override
        public void stop() {
            log.info("stopping");
        }

        @Override
        public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
            if (start.isPresent()) {
                _startOffset = (ExampleDBOffset)start.get();
                log.info("setting offset range: start " + _startOffset);
            } else {
                _startOffset = new ExampleDBOffset(0);
                log.info("computed offset range: start " + _startOffset);
            }
            if (end.isPresent()) {
                log.info("setting offset range: end " + _endOffset);
            } else {
                DBClientWrapper db = new DBClientWrapper(_host, _port);
                db.connect();
                try {
                    List<Split> splits = db.getSplits(_table, 1);
                    _endOffset = new ExampleDBOffset(((SimpleSplit)splits.get(0)).lastRow());
                    log.info("computed offset range: end " + _endOffset);
                } catch (UnknownTableException ute) {
                    throw new RuntimeException(ute);
                } finally {
                    db.disconnect();
                }
            }

        }

        @Override
        public Offset getStartOffset() {
            log.info("getting start offset: " + _startOffset);
            return _startOffset;
        }

        @Override
        public Offset getEndOffset() {
            log.info("getting end offset: " + _endOffset);
            return _endOffset;
        }

        @Override
        public Offset deserializeOffset(String json) {
            log.info("deserialize offset: " + json);
            String last = json.substring(json.lastIndexOf(':')+1);
            long o = Long.parseLong(last.substring(0, last.length() - 1));
            return new ExampleDBOffset(o);
        }

        @Override
        public void commit(Offset end) {
            log.info("commit: " + end);
        }

    }

    /**
     * This is used by a single executor to read from ExampleDB. Notice how it
     * reads all of the data since it knows that only one instance will exist at a time --
     * again because this DataSource only supports sequential data access.
     * Also note that when DBClientWrapper's getTableReader() method is called
     * it reads ALL the data in the table eagerly.
     */
    static class TaskDataReader implements DataReader<Row> {

        static Logger log = Logger.getLogger(TaskDataReader.class.getName());

        public TaskDataReader(String host, int port, String table, StructType schema,
                              ExampleDBOffsetRange offsetRange)
                throws UnknownTableException {
            log.info("Task reading from [" + host + ":" + port + "]" );
            _db = new DBClientWrapper(host, port);
            _db.connect();
            _reader = _db.getTableReader(table, schema.fieldNames()); // create a split from the offset range (sigh!)
        }

        private DBClientWrapper _db;

        private DBTableReader _reader;

        @Override
        public boolean next() {
            return _reader.next();
        }

        @Override
        public Row get() {
            return _reader.get();
        }

        @Override
        public void close() throws IOException {
            _db.disconnect();
        }
    }

    /**
     * Note that this has to be serializable. Each instance is sent to an executor,
     * which uses it to create a reader for its own use.
     */
    static class SimpleDataReaderFactory implements DataReaderFactory<Row> {

        static Logger log = Logger.getLogger(SimpleDataReaderFactory.class.getName());

        public SimpleDataReaderFactory(String host, int port,
                                       String table, StructType schema,
                                       ExampleDBOffsetRange offsetRange) {
            _host = host;
            _port = port;
            _table = table;
            _schema = schema;
            _offsetRange = offsetRange;
        }

        private String _host;
        private int _port;
        private String _table;
        private StructType _schema;
        private ExampleDBOffsetRange _offsetRange;

        @Override
        public DataReader<Row> createDataReader() {
            log.info("Factory creating reader for [" + _host + ":" + _port + "]" );
            try {
                return new TaskDataReader(_host, _port, _table, _schema, _offsetRange);
            } catch (UnknownTableException ute) {
                throw new RuntimeException(ute);
            }
        }

    }


}
