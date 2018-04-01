package edb.server;


import edb.common.Row;
import edb.common.Schema;
import edb.common.Split;

import java.util.List;

public interface ITable {

    String getName();

    Schema getSchema();

    void addRows(List<Row> rows);

    List<Row> getRows();

    List<Row> getRows(Split split);

    long getRowCount();

    List<Split> makeSplits();

    List<Split> makeSplits(int countDesired);
}
