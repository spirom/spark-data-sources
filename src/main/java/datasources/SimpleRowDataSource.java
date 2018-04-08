package datasources;

import datasources.utils.DBClientWrapper;
import datasources.utils.DBTableReader;
import org.apache.log4j.Logger;

import edb.common.UnknownTableException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

/**
 * An extremely simple DataSource that supports sequential reads (i.e.: on just one executor)
 * from the ExampleDB. It only supports reads from a single, pre-defined table with a
 * pre-defined schema. This DataSource is probably about a simple as one that reads from a
 * remote database can get.
 */
public class SimpleRowDataSource implements DataSourceV2, ReadSupport {

    static Logger log = Logger.getLogger(SimpleRowDataSource.class.getName());

    /**
     * Spark calls this to create the reader. Notice how it pulls the host and port
     * on which ExampleDB is listening from the supplied options.
     * @param options
     * @return
     */
    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        String host = options.get("host").orElse("localhost");
        int port = options.getInt("port", -1);
        return new Reader(host, port);
    }

    /**
     * This is how Spark discovers the source table's schema (fixed in this case) and how it
     * obtains the reader factories to be used by the executors to create readers. In this
     * case only one reader factory is created, supporting just one executor, so the
     * resulting Dataset will have only a single partition -- that's why this DataSource
     * only provides sequential reads.
     */
    static class Reader implements DataSourceReader {

        static Logger log = Logger.getLogger(Reader.class.getName());

        public Reader(String host, int port) {
            _host = host;
            _port = port;
        }

        private final StructType schema = new StructType().add("i", "bigint").add("j", "bigint");
        private String _host;
        private int _port;

        @Override
        public StructType readSchema() {
            return schema;
        }

        @Override
        public List<DataReaderFactory<Row>> createDataReaderFactories() {
            log.info("creating a single factory");
            return java.util.Arrays.asList(new SimpleDataReaderFactory(_host, _port));
        }
    }

    /**
     * This is used by a single executor to read from ExampleDB. Notice how in
     * this case it reads from only a single table and reads all of the data since
     * it knows that only one instance will exist at a time -- again because this DataSource
     * only supports sequential data access. Also note that when DBClientWrapper's
     * getTableReader() method is called it reads ALL the data in the table eagerly.
     */
    static class TaskDataReader implements DataReader<Row> {

        static Logger log = Logger.getLogger(TaskDataReader.class.getName());

        public TaskDataReader(String host, int port) throws UnknownTableException {
            log.info("Task reading from [" + host + ":" + port + "]" );
            _db = new DBClientWrapper(host, port);
            _db.connect();
            _reader = _db.getTableReader(tableName, new String[]{"i", "j"});
        }

        private DBClientWrapper _db;

        private DBTableReader _reader;

        private final static String tableName = "theTable";

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

        public SimpleDataReaderFactory(String host, int port) {
            _host = host;
            _port = port;
        }

        private String _host;
        private int _port;

        @Override
        public DataReader<Row> createDataReader() {
            log.info("Factory creating reader for [" + _host + ":" + _port + "]" );
            try {
                return new TaskDataReader(_host, _port);
            } catch (UnknownTableException ute) {
                throw new RuntimeException(ute);
            }
        }

    }


}
