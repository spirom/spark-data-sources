package edb.server;

import edb.common.Filter;
import edb.common.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Filtering and projection operations on a set ot rows, applied after the rows are fetched
 * from a table.
 *
 * NOTE: This is absolutely NOT the way any sensible DBMS would implement predicate and projection
 * push-down. THe whole point is to apply them to avoid reading the data in the first place, rather
 * than to read it and then discard it. But ExampleDB is not intended to be a sensible or
 * or high-performance DBMS -- it's an expedient implementation of a an interface to explore developing
 * Spark external data sources.
 *
 */
public class Rowset {
    public Rowset(List<Row> rows) {
        _rows = rows;
    }

    public List<Row> getRows() { return _rows; }

    public void applyFilter(Filter filter) {

    }

    public void applyProjection(List<String> columns) {
        List<Row> replacementRows = new ArrayList<>();
        for (Row row : _rows) {
            Row replacementRow = new Row();
            for (String column : columns) {
                replacementRow.addField(row.getField(column));
            }
            replacementRows.add(replacementRow);
        }
        _rows = replacementRows;
    }

    private List<Row> _rows;

}
