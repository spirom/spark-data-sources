package edb.common;


import edb.rpc.ColumnSchema;
import edb.rpc.TableSchema;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Schema  implements Serializable {

    public enum ColumnType {
        INT64,
        DOUBLE,
        STRING
    }

    public static class SchemaEntry implements Serializable {
        public SchemaEntry(String name, ColumnType type) {
            _name = name;
            _type = type;
        }

        public String getName() { return _name; }

        public ColumnType getType() { return _type; }

        private String _name;
        private ColumnType _type;
    }

    public Schema() {

    }

    public boolean isCompatible(Schema other) {
        if (_orderedColumns.size() != other._orderedColumns.size()) return false;
        for (int i = 0; i < _orderedColumns.size(); i++) {
            if (!_orderedColumns.get(i)._name.equals(other._orderedColumns.get(i)._name)) return false;
            if (_orderedColumns.get(i)._type != other._orderedColumns.get(i)._type) return false;
        }
        return true;
    }

    public Schema(TableSchema rpcSchema) {
        for (ColumnSchema rpcColumn : rpcSchema.getColumnList()) {
            switch (rpcColumn.getType()) {
                case Int64:
                    addColumn(rpcColumn.getName(), Schema.ColumnType.INT64);
                    break;
                case Double:
                    addColumn(rpcColumn.getName(), Schema.ColumnType.DOUBLE);
                    break;
                case String:
                    addColumn(rpcColumn.getName(), Schema.ColumnType.STRING);
                    break;
            }
        }
    }

    public void addColumn(String name, ColumnType type) {
        SchemaEntry entry = new SchemaEntry(name, type);
        _columns.put(name, entry);
        _orderedColumns.add(entry);
    }

    public void build(TableSchema.Builder builder) {
        Vector<ColumnSchema> convertedColumns = new Vector<>();
        for (SchemaEntry e : _orderedColumns) {
            ColumnSchema.Builder colBuilder = ColumnSchema.newBuilder();
            colBuilder.setName(e.getName());
            switch (e.getType()) {
                case INT64:
                    colBuilder.setType(edb.rpc.ColumnType.Int64);
                    break;
                case DOUBLE:
                    colBuilder.setType(edb.rpc.ColumnType.Double);
                    break;
                case STRING:
                    colBuilder.setType(edb.rpc.ColumnType.String);
                    break;
                default:
            }
            convertedColumns.add(colBuilder.build());
        }
        builder.addAllColumn(convertedColumns);
    }

    public int getColumnCount() { return _orderedColumns.size(); }

    public String getColumnName(int i) { return _orderedColumns.get(i).getName(); }

    public ColumnType getColumnType(int i) { return _orderedColumns.get(i).getType(); }

    public ColumnType getColumnType(String name) { return _columns.get(name).getType(); }

    private Hashtable<String, SchemaEntry> _columns = new Hashtable<>();

    private Vector<SchemaEntry> _orderedColumns = new Vector<>();
}
