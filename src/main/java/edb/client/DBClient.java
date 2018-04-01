package edb.client;

import com.carrotsearch.hppc.ByteScatterSet;
import com.google.protobuf.ByteString;
import edb.common.*;
import edb.common.Split;
import edb.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;

public class DBClient implements IExampleDB {

    private final ManagedChannel _channel;
    private final ExampleDBGrpc.ExampleDBBlockingStub _blockingStub;

    public DBClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    public DBClient(ManagedChannel channel) {
        _channel = channel;
        _blockingStub = ExampleDBGrpc.newBlockingStub(channel);
    }

    public String ping(String id) {
        PingRequest.Builder builder = PingRequest.newBuilder();
        builder.setId(id);

        PingRequest request = builder.build();
        PingResponse response;
        try {
            response = _blockingStub.ping(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            return null;
        }

        return response.getId();
    }

    public List<String> listTables() {
        ListTablesRequest.Builder builder = ListTablesRequest.newBuilder();

        ListTablesRequest request = builder.build();
        ListTablesResponse response;
        try {
            response = _blockingStub.listTables(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            return null;
        }

        return response.getTableNamesList();
    }

    public void createTable(String name, Schema schema) throws ExistingTableException {
        CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
        builder.setName(name);
        TableSchema.Builder schemaBuilder = TableSchema.newBuilder();
        schema.build(schemaBuilder);
        builder.setSchema(schemaBuilder.build());

        CreateTableRequest request = builder.build();
        CreateTableResponse response;
        try {
            response = _blockingStub.createTable(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (!response.getResult()) {
            throw new ExistingTableException(name);
        }
    }

    public void createTable(String name, Schema schema, String clusterColumn)
            throws ExistingTableException {
        CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
        builder.setName(name);
        TableSchema.Builder schemaBuilder = TableSchema.newBuilder();
        schema.build(schemaBuilder);
        builder.setSchema(schemaBuilder.build());
        builder.setClusterColumn(clusterColumn);

        CreateTableRequest request = builder.build();
        CreateTableResponse response;
        try {
            response = _blockingStub.createTable(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (!response.getResult()) {
            throw new ExistingTableException(name);
        }
    }

    public Schema getTableSchema(String name) throws UnknownTableException {
        GetTableSchemaRequest.Builder builder = GetTableSchemaRequest.newBuilder();
        builder.setName(name);

        GetTableSchemaRequest request = builder.build();
        GetTableSchemaResponse response;
        try {
            response = _blockingStub.getTableSchema(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (response.getResult()) {
            Schema schema = new Schema(response.getSchema());
            return schema;
        } else {
            throw new UnknownTableException(name);
        }
    }

    public void bulkInsert(String name, List<Row> rows) throws UnknownTableException
    {
        BulkInsertRequest.Builder builder = BulkInsertRequest.newBuilder();
        builder.setName(name);

        List<DataRow> rpcRows = new ArrayList<>();
        for (Row row : rows) {
            DataRow.Builder rowBuilder = DataRow.newBuilder();
            row.build(rowBuilder);
            rpcRows.add(rowBuilder.build());
        }
        builder.addAllRow(rpcRows);

        BulkInsertRequest request = builder.build();
        BulkInsertResponse response;
        try {
            response = _blockingStub.bulkInsert(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (!response.getResult()) {
            throw new UnknownTableException(name);
        }
    }

    public List<Row> getAllRows(String name) throws UnknownTableException {
        return getAllRows(name, null);
    }

    public List<Row> getAllRows(String name, Split split) throws UnknownTableException {
        GetAllRowsRequest.Builder builder = GetAllRowsRequest.newBuilder();
        builder.setName(name);

        if (split != null) {
            edb.rpc.Split.Builder splitBuilder = edb.rpc.Split.newBuilder();
            splitBuilder.setOpaque(ByteString.copyFrom(split.serialize()));
            builder.setSplit(splitBuilder.build());
        }

        GetAllRowsRequest request = builder.build();
        GetAllRowsResponse response;
        try {
            response = _blockingStub.getAllRows(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (response.getResult()) {
            List<Row> rows = new ArrayList<>();
            List<DataRow> rpcRows = response.getRowList();
            for (DataRow rpcRow : rpcRows) {
                rows.add(new Row(rpcRow));
            }
            return rows;
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Split> getSplits(String table) throws UnknownTableException {
        return getSplits(table, 0);
    }

    public List<Split> getSplits(String table, int count) throws UnknownTableException {
        GetSplitsRequest.Builder builder = GetSplitsRequest.newBuilder();
        builder.setName(table);

        if (count != 0) {
            builder.setCount(count);
        }

        GetSplitsRequest request = builder.build();
        GetSplitsResponse response;
        try {
            response = _blockingStub.getSplits(request);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        if (response.getResult()) {
            List<Split> splits = new ArrayList<>();
            List<edb.rpc.Split> rpcSplits = response.getSplitsList();
            for (edb.rpc.Split rpcSplit : rpcSplits) {
                byte[] splitBytes = rpcSplit.getOpaque().toByteArray();
                Split split = Split.deserialize(splitBytes);
                splits.add(split);
            }
            return splits;
        } else {
            throw new UnknownTableException(table);
        }
    }
}
