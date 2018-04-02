import edb.common.*;
import edb.server.Database;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import util.SampleTables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LocalDBTest {

    private IExampleDB _db;

    @Before
    public void setUp() {
        _db = new Database();
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testSplitSerDe() {
        SimpleSplit simpleSplit = new SimpleSplit(5, 10);
        byte[] bytes = simpleSplit.serialize();
        Split otherSplit = Split.deserialize(bytes);
        assertThat(otherSplit).isInstanceOf(SimpleSplit.class);
        SimpleSplit otherSimpleSplit = (SimpleSplit) otherSplit;
        assertThat(otherSimpleSplit.firstRow()).isEqualTo(5);
        assertThat(otherSimpleSplit.lastRow()).isEqualTo(10);
    }

    @Test
    public void testListNoTables() throws IOException {
        List<String> names = _db.listTables();
        assertThat(names).isNotNull();
        assertThat(names.size()).isZero();
    }

    @Test
    public void testCreateListTables() throws IOException, ExistingTableException {
        _db.createTable("table1", new Schema());
        _db.createTable("table2", new Schema());
        List<String> names = _db.listTables();
        assertThat(names).isNotNull();
        assertThat(names.size()).isEqualTo(2);
    }

    @Test
    public void testCreateExistingTable() throws IOException, ExistingTableException {
        _db.createTable("table1", new Schema());
        assertThatThrownBy(() -> {
            _db.createTable("table1", new Schema());
                }).isInstanceOf(ExistingTableException.class).hasMessageContaining("table1");
            List<String> names = _db.listTables();
            assertThat(names).isNotNull();
            assertThat(names.size()).isEqualTo(1);
        }

        @Test
    public void testGetUnknownTableSchema() throws IOException {
        assertThatThrownBy(() -> {
                    Schema retSchema = _db.getTableSchema("table1");
                }).isInstanceOf(UnknownTableException.class).hasMessageContaining("table1");
    }

    @Test
    public void testSetGetTableSchema()
            throws IOException, UnknownTableException, ExistingTableException {

        Schema schema = new Schema();
        schema.addColumn("a", Schema.ColumnType.INT64);
        schema.addColumn("b", Schema.ColumnType.INT64);
        schema.addColumn("c", Schema.ColumnType.DOUBLE);

        _db.createTable("table1", schema);

        Schema retSchema = _db.getTableSchema("table1");
        assertThat(retSchema).isNotNull();

        assertThat(retSchema.getColumnCount()).isEqualTo(3);

        assertThat(retSchema.getColumnName(0)).isEqualTo("a");
        assertThat(retSchema.getColumnType(0)).isEqualTo(Schema.ColumnType.INT64);

        assertThat(retSchema.getColumnName(1)).isEqualTo("b");
        assertThat(retSchema.getColumnType(1)).isEqualTo(Schema.ColumnType.INT64);

        assertThat(retSchema.getColumnName(2)).isEqualTo("c");
        assertThat(retSchema.getColumnType(2)).isEqualTo(Schema.ColumnType.DOUBLE);
    }

    @Test
    public void testCreateInsertScan()
            throws IOException, UnknownTableException, ExistingTableException {

        Schema schema = new Schema();
        schema.addColumn("a", Schema.ColumnType.INT64);
        schema.addColumn("b", Schema.ColumnType.INT64);
        schema.addColumn("c", Schema.ColumnType.DOUBLE);

        _db.createTable("table1", schema);

        List<Row> toInsert = new ArrayList<>();
        Row r1 = new Row();
        r1.addField(new Row.Int64Field("a", 100));
        r1.addField(new Row.Int64Field("b", 200));
        r1.addField(new Row.DoubleField("c", 2.1));
        toInsert.add(r1);
        Row r2 = new Row();
        r2.addField(new Row.Int64Field("a", 300));
        r2.addField(new Row.Int64Field("b", 400));
        r2.addField(new Row.DoubleField("c", 1.1));
        toInsert.add(r2);

        _db.bulkInsert("table1", toInsert);

        List<Row> returned = _db.getAllRows("table1");

        assertThat(returned).isNotNull();

        assertThat(returned.size()).isEqualTo(2);
    }

    @Test
    public void testTooManySplits() throws Exception {

        SampleTables.makeSimple(_db, "table1", 3);

        List<Split> splits = _db.getSplits("table1", 5);

        assertThat(splits).isNotNull();

        assertThat(splits.size()).isEqualTo(5);

        SimpleSplit simpleSplit = (SimpleSplit)splits.get(0);
        assertThat(simpleSplit.firstRow()).isEqualTo(0);
        assertThat(simpleSplit.lastRow()).isEqualTo(1);

        List<Row> returned = _db.getAllRows("table1", splits.get(0));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(1);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(0);

        simpleSplit = (SimpleSplit)splits.get(1);
        assertThat(simpleSplit.firstRow()).isEqualTo(1);
        assertThat(simpleSplit.lastRow()).isEqualTo(2);

        returned = _db.getAllRows("table1", splits.get(1));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(1);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(1);

        simpleSplit = (SimpleSplit)splits.get(2);
        assertThat(simpleSplit.firstRow()).isEqualTo(2);
        assertThat(simpleSplit.lastRow()).isEqualTo(3);
        assertThat(simpleSplit.lastRow()).isEqualTo(3);

        returned = _db.getAllRows("table1", splits.get(2));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(1);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(2);

        assertThat(splits.get(3).isEmpty()).isTrue();

        returned = _db.getAllRows("table1", splits.get(3));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(0);

        assertThat(splits.get(4).isEmpty()).isTrue();

        returned = _db.getAllRows("table1", splits.get(4));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(0);
    }

    @Test
    public void testEvenSplits() throws Exception {

        SampleTables.makeSimple(_db, "table1", 9);

        List<Split> splits = _db.getSplits("table1", 3);

        assertThat(splits).isNotNull();

        assertThat(splits.size()).isEqualTo(3);

        SimpleSplit simpleSplit = (SimpleSplit)splits.get(0);
        assertThat(simpleSplit.firstRow()).isEqualTo(0);
        assertThat(simpleSplit.lastRow()).isEqualTo(3);

        List<Row> returned = _db.getAllRows("table1", splits.get(0));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(3);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(0);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(1);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(2);

        simpleSplit = (SimpleSplit)splits.get(1);
        assertThat(simpleSplit.firstRow()).isEqualTo(3);
        assertThat(simpleSplit.lastRow()).isEqualTo(6);

        returned = _db.getAllRows("table1", splits.get(1));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(3);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(3);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(4);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(5);

        simpleSplit = (SimpleSplit)splits.get(2);
        assertThat(simpleSplit.firstRow()).isEqualTo(6);
        assertThat(simpleSplit.lastRow()).isEqualTo(9);

        returned = _db.getAllRows("table1", splits.get(2));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(3);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(6);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(7);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(8);
    }

    @Test
    public void testUnevenSplits()
            throws Exception {

        SampleTables.makeSimple(_db, "table1", 10);

        List<Split> splits = _db.getSplits("table1", 3);

        assertThat(splits).isNotNull();

        assertThat(splits.size()).isEqualTo(3);

        SimpleSplit simpleSplit = (SimpleSplit)splits.get(0);
        assertThat(simpleSplit.firstRow()).isEqualTo(0);
        assertThat(simpleSplit.lastRow()).isEqualTo(4);

        List<Row> returned = _db.getAllRows("table1", splits.get(0));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(0);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(1);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(2);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(3);

        simpleSplit = (SimpleSplit)splits.get(1);
        assertThat(simpleSplit.firstRow()).isEqualTo(4);
        assertThat(simpleSplit.lastRow()).isEqualTo(8);

        returned = _db.getAllRows("table1", splits.get(1));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(4);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(5);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(6);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(7);

        simpleSplit = (SimpleSplit)splits.get(2);
        assertThat(simpleSplit.firstRow()).isEqualTo(8);
        assertThat(simpleSplit.lastRow()).isEqualTo(10);

        returned = _db.getAllRows("table1", splits.get(2));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(2);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(8);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(9);
    }

    @Test
    public void testTrickySplits()
            throws Exception {

        SampleTables.makeSimple(_db, "table1", 20);

        String clusteredIndexColumn = _db.getTableClusteredIndexColumn("table1");
        assertThat(clusteredIndexColumn).isNull();

        List<Split> splits = _db.getSplits("table1", 6);

        assertThat(splits).isNotNull();

        assertThat(splits.size()).isEqualTo(6);

        SimpleSplit simpleSplit = (SimpleSplit)splits.get(0);
        assertThat(simpleSplit.firstRow()).isEqualTo(0);
        assertThat(simpleSplit.lastRow()).isEqualTo(4);

        simpleSplit = (SimpleSplit)splits.get(1);
        assertThat(simpleSplit.firstRow()).isEqualTo(4);
        assertThat(simpleSplit.lastRow()).isEqualTo(8);

        simpleSplit = (SimpleSplit)splits.get(2);
        assertThat(simpleSplit.firstRow()).isEqualTo(8);
        assertThat(simpleSplit.lastRow()).isEqualTo(12);

        simpleSplit = (SimpleSplit)splits.get(3);
        assertThat(simpleSplit.firstRow()).isEqualTo(12);
        assertThat(simpleSplit.lastRow()).isEqualTo(16);

        simpleSplit = (SimpleSplit)splits.get(4);
        assertThat(simpleSplit.firstRow()).isEqualTo(16);
        assertThat(simpleSplit.lastRow()).isEqualTo(20);

        assertThat(splits.get(5).isEmpty()).isTrue();

    }

    @Test
    public void testClusteredSplits()
            throws Exception {

        SampleTables.makeClustered(_db, "table1", 16, 8);

        String clusteredIndexColumn = _db.getTableClusteredIndexColumn("table1");
        assertThat(clusteredIndexColumn).isNotNull();
        assertThat(clusteredIndexColumn).isEqualTo("c");

        List<Split> splits = _db.getSplits("table1", 4);

        assertThat(splits).isNotNull();

        assertThat(splits.size()).isEqualTo(4);

        IndexSplit indexSplit = (IndexSplit)splits.get(0);
        assertThat(indexSplit.firstRow()).isEqualTo(0);
        assertThat(indexSplit.lastRow()).isEqualTo(2);

        List<Row> returned = _db.getAllRows("table1", splits.get(0));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(0);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(8);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(1);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(9);

        indexSplit = (IndexSplit)splits.get(1);
        assertThat(indexSplit.firstRow()).isEqualTo(2);
        assertThat(indexSplit.lastRow()).isEqualTo(4);

        returned = _db.getAllRows("table1", splits.get(1));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(2);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(10);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(3);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(11);

        indexSplit = (IndexSplit)splits.get(2);
        assertThat(indexSplit.firstRow()).isEqualTo(4);
        assertThat(indexSplit.lastRow()).isEqualTo(6);

        returned = _db.getAllRows("table1", splits.get(2));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(4);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(12);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(5);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(13);

        indexSplit = (IndexSplit)splits.get(3);
        assertThat(indexSplit.firstRow()).isEqualTo(6);
        assertThat(indexSplit.lastRow()).isEqualTo(8);

        returned = _db.getAllRows("table1", splits.get(3));
        assertThat(returned).isNotNull();
        assertThat(returned.size()).isEqualTo(4);
        assertThat(returned.get(0).getField("a").getInt64Value()).isEqualTo(6);
        assertThat(returned.get(1).getField("a").getInt64Value()).isEqualTo(14);
        assertThat(returned.get(2).getField("a").getInt64Value()).isEqualTo(7);
        assertThat(returned.get(3).getField("a").getInt64Value()).isEqualTo(15);

    }

}
