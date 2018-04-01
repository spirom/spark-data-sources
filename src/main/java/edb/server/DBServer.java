package edb.server;

import com.google.protobuf.ByteString;
import edb.common.*;
import edb.rpc.*;

import edb.rpc.Split;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DBServer {

    static Logger log = Logger.getLogger(DBServer.class.getName());

    private int _port = 50051;

    private Server server;

    public DBServer(int port) {
        _port = port;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(_port)
                .addService(new ExampleDBImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.info("shutting down gRPC server since JVM is shutting down");
                DBServer.this.stop();
                log.info("gRPC server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    static class ExampleDBImpl extends ExampleDBGrpc.ExampleDBImplBase {

        Database _db = new Database();

        @Override
        public void ping(PingRequest req, StreamObserver<PingResponse> responseObserver) {

            String id = req.getId();

            PingResponse.Builder builder = PingResponse.newBuilder();
            PingResponse reply = builder.setId(id).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void listTables(ListTablesRequest req,
                               StreamObserver<ListTablesResponse> responseObserver) {


            ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();

            builder.addAllTableNames(_db.listTables());

            ListTablesResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void createTable(CreateTableRequest req,
                                StreamObserver<CreateTableResponse> responseObserver) {

            String name = req.getName();
            TableSchema schema = req.getSchema();

            CreateTableResponse.Builder builder = CreateTableResponse.newBuilder();
            try {
                if (req.hasClusterColumn()) {
                    _db.createTable(name, new Schema(schema), req.getClusterColumn());
                } else {
                    _db.createTable(name, new Schema(schema));
                }
                builder.setResult(true);
            } catch (ExistingTableException ete) {
                builder.setResult(false);
            }

            CreateTableResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void getTableSchema(GetTableSchemaRequest req,
                                   StreamObserver<GetTableSchemaResponse> responseObserver) {

            String name = req.getName();

            GetTableSchemaResponse.Builder builder = GetTableSchemaResponse.newBuilder();
            try {
                Schema schema = _db.getTableSchema(name);
                TableSchema.Builder schemaBuilder = TableSchema.newBuilder();
                schema.build(schemaBuilder);
                TableSchema rpcSchema = schemaBuilder.build();
                builder.setResult(true);
                builder.setSchema(rpcSchema);

            } catch (UnknownTableException ete) {
                builder.setResult(false);
            }

            GetTableSchemaResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void bulkInsert(BulkInsertRequest req,
                               StreamObserver<BulkInsertResponse> responseObserver) {
            String name = req.getName();

            BulkInsertResponse.Builder builder = BulkInsertResponse.newBuilder();
            try {
                List<Row> rows = new ArrayList<>();
                List<DataRow> rpcRows = req.getRowList();
                for (DataRow rpcRow : rpcRows) {
                    rows.add(new Row(rpcRow));
                }
                _db.bulkInsert(name, rows);
                builder.setResult(true);
            } catch (UnknownTableException ete) {
                builder.setResult(false);
            }

            BulkInsertResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void getAllRows(GetAllRowsRequest req,
                               StreamObserver<GetAllRowsResponse> responseObserver) {
            String name = req.getName();

            GetAllRowsResponse.Builder builder = GetAllRowsResponse.newBuilder();
            try {
                List<Row> rows = null;
                if (req.hasSplit()) {
                    Split rpcSplit = req.getSplit();
                    edb.common.Split split =
                            edb.common.Split.deserialize(rpcSplit.getOpaque().toByteArray());
                    rows = _db.getAllRows(name, split);
                } else {
                    rows = _db.getAllRows(name);
                }
                List<DataRow> rpcRows = new ArrayList<>();
                for (Row row : rows) {
                    DataRow.Builder rowBuilder = DataRow.newBuilder();
                    row.build(rowBuilder);
                    rpcRows.add(rowBuilder.build());
                }
                builder.addAllRow(rpcRows);
                builder.setResult(true);
            } catch (UnknownTableException ete) {
                builder.setResult(false);
            }

            GetAllRowsResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void getSplits(GetSplitsRequest req,
                              StreamObserver<GetSplitsResponse> responseObserver) {
            String name = req.getName();

            GetSplitsResponse.Builder builder = GetSplitsResponse.newBuilder();
            try {
                List<edb.common.Split> splits = null;
                if (req.hasCount()) {
                    splits = _db.getSplits(name, req.getCount());
                } else {
                    splits = _db.getSplits(name);
                }
                List<Split> rpcSplits = new ArrayList<>();
                for (edb.common.Split split : splits) {
                    edb.rpc.Split.Builder splitBuilder = edb.rpc.Split.newBuilder();
                    byte[] splitBytes = split.serialize();
                    splitBuilder.setOpaque(ByteString.copyFrom(splitBytes));
                    rpcSplits.add(splitBuilder.build());
                }
                builder.addAllSplits(rpcSplits);
                builder.setResult(true);
            } catch (UnknownTableException ete) {
                builder.setResult(false);
            }

            GetSplitsResponse reply = builder.build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

    }
}


