package datasources;

import datasources.utils.DBClientWrapper;
import datasources.utils.DBTableReader;
import edb.common.Schema;
import edb.common.Split;
import edb.common.UnknownTableException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import sun.security.pkcs11.wrapper.CK_ATTRIBUTE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This data source adds the ability to write data, and does so in parallel.
 * The various classes for reading are identical to those of ParallelRowDataSource. All four values of
 * SaveMode are supported. Each task writes to its own temporary table, and on global commit
 * all of these temporary tables are copied into the destination table in a single
 * ExampleDB transaction.
 */
public class ParallelRowReadWriteDataSource implements DataSourceV2, ReadSupport, WriteSupport {

    static Logger log = Logger.getLogger(ParallelRowReadWriteDataSource.class.getName());

    /**
     * Spark calls this to create the reader. Notice how it pulls the host and port
     * on which ExampleDB is listening, as well as a table name, from the supplied options.
     * @param options
     * @return
     */
    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        String host = options.get("host").orElse("localhost");
        int port = options.getInt("port", -1);
        String table = options.get("table").orElse("unknownTable"); // TODO: throw
        int partitions = Integer.parseInt(options.get("partitions").orElse("0"));
        return new Reader(host, port, table, partitions);
    }

    /**
     * Spark calls this to create the writer. The data source options are used
     * in the same way as above.
     * @param jobId
     * @param schema
     * @param mode
     * @param options
     * @return
     */
    @Override
    public Optional<DataSourceWriter> createWriter(
            String jobId, StructType schema, SaveMode mode, DataSourceOptions options)
    {
        // TODO: ned to distinguish between creating the table for the first time
        // TODO: (just validate schema and create) vs appending (compare schema)

        // TODO: log JobId here and elsewhere whent he partitionId etc are logged

        String host = options.get("host").orElse("localhost");
        int port = options.getInt("port", -1);
        String table = options.get("table").orElse("unknownTable"); // TODO: throw
        int partitions = Integer.parseInt(options.get("partitions").orElse("0"));

        edb.common.Schema dbSchema = DBClientWrapper.sparkToDbSchema(schema);

        boolean truncateOnCommit = false;

        DBClientWrapper db = new DBClientWrapper(host, port);
        db.connect();
        if (db.tableExists(table)) {
            switch (mode) {
                case ErrorIfExists: {
                    // check existence and throw if needed
                    throw new RuntimeException("data already exists");
                }
                case Append: {
                    // just check schema compatibility
                    try {
                        Schema actualSchema = db.getDBSchema(table);
                        if (!dbSchema.isCompatible(actualSchema)) {
                            throw new RuntimeException("Appending to table with incompatible schema");
                        }
                    } catch (UnknownTableException ute) {
                        throw new RuntimeException(ute);
                    }
                    break;
                }
                case Overwrite: {
                    // two options if table exists: truncate it now or truncate it later
                    truncateOnCommit = true;
                    break;
                }
                case Ignore: {
                    // check existence and declare victory
                    return Optional.empty();
                }
                default:
            }
        } else {
            db.createTable(table, dbSchema);
        }

        return Optional.of(new Writer(host, port, table, partitions, dbSchema, truncateOnCommit));
    }

    /**
     * This is defined to package up whatever we want to communicate between a the set of committed
     * local transactions (att he task level) to a global transaction that is being either committed or
     * aborted. In our case its just the name of the temporary table used for each local transaction,
     * but Spark allows it to be anything. In general the contents depend on what your strategy is
     * for making commits transactional.
     */
    static class PartialCommit implements WriterCommitMessage
    {
        public PartialCommit(String tempTableName) {
            _tempTableName = tempTableName;
        }

        public String getTempTableName() { return _tempTableName; }

        private String _tempTableName;
    }

    static class Writer implements DataSourceWriter {

        static Logger log = Logger.getLogger(Writer.class.getName());


        public Writer(String host, int port, String table, int partitions, Schema schema,
                      boolean truncateOnCommit) {
            _host = host;
            _port = port;
            _table = table;
            _partitions = partitions;
            _schema = schema;
            _truncateOnCommit = truncateOnCommit;

        }

        private Schema _schema;
        private String _host;
        private int _port;
        private String _table;
        private int _partitions;
        private boolean _truncateOnCommit;

        @Override
        public DataWriterFactory<Row> createWriterFactory() {
            DataWriterFactory<Row> factory =
                    new RowDataWriterFactory(_host, _port, _table, _schema);
            return factory;
        }

        @Override
        public void commit(WriterCommitMessage[] messages) {
            log.info("before global commit: table " + _table +
                    " from " + messages.length + " local commits");
            // pull out the temp table names
            List<String> sourceTables = new ArrayList<>();
            for (WriterCommitMessage wcm : messages) {
                sourceTables.add(((PartialCommit) wcm).getTempTableName());
            }
            // append them all to the destination table atomically
            DBClientWrapper db = new DBClientWrapper(_host, _port);
            db.connect();
            db.bulkInsertFromTables(_table, _truncateOnCommit, sourceTables);
            log.info("after global commit: table " + _table +
                    " from " + messages.length + " local commits");
        }

        @Override
        public void abort(WriterCommitMessage[] messages) {
            log.info("before global abort: table " + _table +
                    " from " + messages.length + " local commits");
            // TODO: blow away all the temp tables
            log.info("after global abort: table " + _table +
                    " from " + messages.length + " local commits");
        }

    }

    /**
     * This supports the creation of a TaskDataWriter (defined below) for each task, where each
     * task corresponds to some numbered attempt at writing the contents of exactly one partition.
     */
    static class RowDataWriterFactory implements DataWriterFactory<Row> {

        static Logger log = Logger.getLogger(RowDataWriterFactory.class.getName());

        public RowDataWriterFactory(String host, int port,
                                    String table, Schema schema) {
            _host = host;
            _port = port;
            _table = table;
            _schema = schema;
        }

        private String _host;
        private int _port;
        private String _table;
        private Schema _schema;
        private Split _split;

        @Override
        public DataWriter<Row> createDataWriter(int partitionId, int attemptNumber)
        {
            try {
                log.info("creating a data writer for table " + _table +
                        " partition " + partitionId + " attempt " + attemptNumber);
                return new TaskDataWriter(_host, _port, _table, _schema, partitionId, attemptNumber);
            } catch (UnknownTableException ute) {
                throw new RuntimeException(ute);
            }
        }
    }

    /**
     * The task level writer maintains a local buffer of uncommitted records, which are
     * written to a temporary table on a local commit, or simply discarded on an abort.
     * Spark takes responsibility for conveying the WriterCommitMessage, which is actually a
     * PartialCommit as defined above, to the global commit() and abort() methods in the Writer above.
     */
    static class TaskDataWriter implements DataWriter<Row> {

        static Logger log = Logger.getLogger(TaskDataWriter.class.getName());

        private List<edb.common.Row> _uncommitted = new ArrayList<>();
        private String _tempTableName;
        private DBClientWrapper _db;
        private Schema _schema;
        private int _partitionId;
        private int _attemptNumber;

        public TaskDataWriter(String host, int port, String table,
                              Schema schema, int partitionId, int attemptNumber)
                throws UnknownTableException {
            log.info("Task reading from [" + host + ":" + port + "]" );
            _db = new DBClientWrapper(host, port);
            _db.connect();
            _schema = schema;
            _partitionId = partitionId;
            _attemptNumber = attemptNumber;
        }

        @Override
        public void write(Row row) {
            // converting the row to ExampleDB row here for immediate feedback if it fails to convert
            //println("*** task level write")
            _uncommitted.add(DBClientWrapper.sparkToDBRow(row, _schema));
        }

        @Override
        public WriterCommitMessage commit() {
            log.info("before local commit: partition " + _partitionId + " attempt " + _attemptNumber);
            _tempTableName = _db.saveToTempTable(_uncommitted, _schema);
            _uncommitted.clear();
            PartialCommit pc = new PartialCommit(_tempTableName);
            log.info("before local commit: table " + _tempTableName +
                    " partition " + _partitionId + " attempt " + _attemptNumber);
            return pc;
        }

        @Override
        public void abort()
        {
            log.info("before local abort: partition " + _partitionId + " attempt " + _attemptNumber);
            // note we haven't even created the temp table
            _uncommitted.clear();
            log.info("after local abort: partition " + _partitionId + " attempt " + _attemptNumber);
        }
    }

    /**
     * This is how Spark discovers the source table's schema by requesting it from ExmapleDB,
     * and how it obtains the reader factories to be used by the executors to create readers.
     * Notice that one factory is created for each partition.
     */
    static class Reader implements DataSourceReader {

        static Logger log = Logger.getLogger(Reader.class.getName());

        public Reader(String host, int port, String table, int partitions) {
            _host = host;
            _port = port;
            _table = table;
            _partitions = partitions;
        }

        private StructType _schema;
        private String _host;
        private int _port;
        private String _table;
        private int _partitions;

        @Override
        public StructType readSchema() {
            if (_schema == null) {
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
            List<Split> splits = null;
            DBClientWrapper db = new DBClientWrapper(_host, _port);
            db.connect();
            try {
                if (_partitions == 0)
                    splits = db.getSplits(_table);
                else
                    splits = db.getSplits(_table, _partitions);
            } catch (UnknownTableException ute) {
                throw new RuntimeException(ute);
            } finally {
                db.disconnect();
            }
            List<DataReaderFactory<Row>> factories = new ArrayList<>();
            for (Split split : splits) {
                DataReaderFactory<Row> factory =
                        new SplitDataReaderFactory(_host, _port, _table, readSchema(), split);
                factories.add(factory);
            }
            log.info("created " + factories.size() + " factories");
            return factories;
        }
    }

    /**
     * This is used by each executor to read from ExampleDB. It uses the Split to know
     * which data to read.
     * Also note that when DBClientWrapper's getTableReader() method is called
     * it reads ALL the data in its own Split eagerly.
     */
    static class TaskDataReader implements DataReader<Row> {

        static Logger log = Logger.getLogger(TaskDataReader.class.getName());

        public TaskDataReader(String host, int port, String table,
                              StructType schema, Split split)
                throws UnknownTableException {
            log.info("Task reading from [" + host + ":" + port + "]" );
            _db = new DBClientWrapper(host, port);
            _db.connect();
            _reader = _db.getTableReader(table, schema.fieldNames(), split);
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
    static class SplitDataReaderFactory implements DataReaderFactory<Row> {

        static Logger log = Logger.getLogger(SplitDataReaderFactory.class.getName());

        public SplitDataReaderFactory(String host, int port,
                                       String table, StructType schema,
                                       Split split) {
            _host = host;
            _port = port;
            _table = table;
            _schema = schema;
            _split = split;
        }

        private String _host;
        private int _port;
        private String _table;
        private StructType _schema;
        private Split _split;

        @Override
        public DataReader<Row> createDataReader() {
            log.info("Factory creating reader for [" + _host + ":" + _port + "]" );
            try {
                return new TaskDataReader(_host, _port, _table, _schema, _split);
            } catch (UnknownTableException ute) {
                throw new RuntimeException(ute);
            }
        }

    }


}
