package edb.common;

import edb.rpc.DataRow;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Row {
    public static class Field {
        public Field(String name) {
            _name = name;
        }

        public String getName() { return _name; };

        public long getInt64Value() throws InvalidTypeException {
            throw new InvalidTypeException(_name);
        }

        public double getDoubleValue() throws InvalidTypeException {
            throw new InvalidTypeException(_name);
        }

        public String getStringValue() throws InvalidTypeException {
            throw new InvalidTypeException(_name);
        }

        private String _name;
    }

    public static class Int64Field extends Field {
        public Int64Field(String name, long value) {
            super(name);
            _value = value;
        }

        @Override
        public long getInt64Value() throws InvalidTypeException {
            return _value;
        }

        public long getValue() { return _value; }

        private long _value;
    }

    public static class DoubleField extends Field {
        public DoubleField(String name, double value) {
            super(name);
            _value = value;
        }

        @Override
        public double getDoubleValue() throws InvalidTypeException {
            return _value;
        }

        public double getValue() { return _value; }

        private double _value;
    }

    public static class StringField extends Field {
        public StringField(String name, String value) {
            super(name);
            _value = value;
        }

        @Override
        public String getStringValue() throws InvalidTypeException {
            return _value;
        }

        public String getValue() { return _value; }

        private String _value;
    }

    public Row() {}

    public Row(DataRow rpcRow) {
        for (DataRow.KeyValue kv : rpcRow.getKeyValueList()) {
            if (kv.hasDoubleval()) {
                addField(new DoubleField(kv.getKey(), kv.getDoubleval()));
            } else if (kv.hasIn64Val()) {
                addField(new Int64Field(kv.getKey(), kv.getIn64Val()));
            } else if (kv.hasStringval()) {
                addField(new StringField(kv.getKey(), kv.getStringval()));
            }
        }
    }

    public void build(DataRow.Builder builder) {
        Vector<DataRow.KeyValue> convertedColumns = new Vector<>();
        for (Field f : _fieldsByPosition) {
            DataRow.KeyValue.Builder kvBuilder = DataRow.KeyValue.newBuilder();
            kvBuilder.setKey(f.getName());
            if (f instanceof DoubleField) {
                DoubleField df = (DoubleField) f;
                kvBuilder.setDoubleval(df.getValue());
            } else if (f instanceof Int64Field) {
                Int64Field i64f = (Int64Field) f;
                kvBuilder.setIn64Val(i64f.getValue());
            } else if (f instanceof StringField) {
                StringField sf = (StringField) f;
                kvBuilder.setStringval(sf.getValue());
            }
            convertedColumns.add(kvBuilder.build());
        }
        builder.addAllKeyValue(convertedColumns);
    }

    public void addField(Field field) {
        _fieldsByPosition.add(field);
        _fieldsByName.put(field.getName(), field);
    }

    public int getFieldCount() { return _fieldsByPosition.size(); }

    public Field getField(int i) {
        return _fieldsByPosition.get(i);
    }

    public Field getField(String name) {
        return _fieldsByName.get(name);
    }

    private List<Field> _fieldsByPosition = new ArrayList<>();
    private Hashtable<String, Field> _fieldsByName = new Hashtable<>();
}
