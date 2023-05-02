package tech.tablesaw.io.ods;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static tech.tablesaw.aggregate.AggregateFunctions.mean;
import static tech.tablesaw.api.ColumnType.*;
import static tech.tablesaw.api.ColumnType.STRING;

public class OdsReaderTest {

  @Test
  public void immunizationTest() {
    Table table = new OdsReader().read(OdsReadOptions.builder("../data/immunization.ods").build());
    assertEquals(8, table.columnCount(), "Number of columns");
    assertEquals(70, table.rowCount(), "Number of rows");
    Table summary = table.summarize("BCGCoverage", mean).by("OrgUnitName");
    assertEquals(113.0,
        summary.where(summary.stringColumn("OrgUnitName").isEqualTo("Bonthe"))
            .get(0, 1)
    );
  }

  private List<Table> readN(String name, int expectedCount, boolean removeEmptyColumns, Map<String, ColumnType>... columnTypes) {
    try {
      String fileName = name + ".ods";
      var builder = OdsReadOptions
          .builder("../data/" + fileName)
          .removeEmptyColumns(removeEmptyColumns);
      if (columnTypes.length > 0) {
        builder.columnTypesPartial(columnTypes[0]);
      }
      List<Table> tables = new OdsReader().readMultiple(builder.build());
      assertNotNull(tables, "No tables read from " + fileName);
      assertEquals(expectedCount, tables.size(), "Wrong number of tables in " + fileName);
      return tables;
    } catch (final IOException e) {
      fail(e.getMessage());
    }
    return null;
  }

  private Table read1(String name, int size, List<String> columnNames, boolean removeEmptyColumns, Map<String, ColumnType>... columnTypes) {
    Table table = readN(name, 1, removeEmptyColumns, columnTypes).get(0);
    int colNum = 0;
    for (final Column<?> column : table.columns()) {
      assertEquals(columnNames.get(colNum), column.name(), "Wrong column name");
      assertEquals(size, column.size(), "Wrong size for column " + columnNames.get(colNum));
      colNum++;
    }
    return table;
  }

  @SafeVarargs
  private <T> void assertColumnValues(Column<T> column, T... ts) {
    for (int i = 0; i < column.size(); i++) {
      if (ts[i] == null) {
        assertTrue(
            column.isMissing(i),
            "Should be missing value in row "
                + i
                + " of column "
                + column.name()
                + ", but it was "
                + column.get(i));
      } else {
        assertEquals(
            ts[i], column.get(i), "Wrong value in row " + i + " of column " + column.name());
      }
    }
  }

  @Test
  public void testColumns() {
    Table table =
        read1(
            "columns",
            3,
            List.of("stringcol",
            "shortcol",
            "intcol",
            "longcol",
            "doublecol",
            "booleancol",
            "datecol",
            "formulacol",
            "mixed",
            "mixed2",
            "intcol2"),
            true
        );
    assertEquals(
        List.of(STRING, INTEGER, INTEGER, LONG, DOUBLE, BOOLEAN, LOCAL_DATE_TIME, DOUBLE, STRING, STRING, INTEGER),
        table.types(),
        "column types"
    );
    assertColumnValues(table.stringColumn("stringcol"), "Hallvard", "Marit", "Quentin");
    assertColumnValues(table.intColumn("shortcol"), 123, 124, 125);
    assertColumnValues(table.intColumn("intcol"), 12345678, 12345679, 12345679);
    assertColumnValues(table.longColumn("longcol"), 12345678900L, 12345678901L, 12345678901L);
    assertColumnValues(table.doubleColumn("doublecol"), 12.34, 13.35, 13.35);
    assertColumnValues(table.booleanColumn("booleancol"), true, false, false);
    assertColumnValues(
        table.dateTimeColumn("datecol"),
        LocalDateTime.of(2019, 2, 22, 20, 54, 9),
        LocalDateTime.of(2020, 3, 23, 21, 55, 10),
        LocalDateTime.of(2020, 3, 23, 21, 55, 10));
    assertColumnValues(table.doubleColumn("formulacol"), 135.34, 137.35, 138.35);
    assertEquals(Lists.newArrayList("123.0", "abc", ""), table.column("mixed").asList());
    assertEquals(Lists.newArrayList("abc", "123.0", ""), table.column("mixed2").asList());
    assertEquals(Lists.newArrayList(null, 1234, 1234), table.column("intcol2").asList());
  }

  @Test
  public void testColumnsWithMissingValues() {
    Table table =
        read1(
            "columns-with-missing-values",
            2,
            List.of(
            "stringcol",
            "shortcol",
            "intcol",
            "longcol",
            "doublecol",
            "booleancol",
            "datecol",
            "formulacol"),
            true
        );
    assertColumnValues(table.stringColumn("stringcol"), "Hallvard", null);
    assertColumnValues(table.intColumn("shortcol"), null, 124);
    assertColumnValues(table.intColumn("intcol"), 12345678, 12345679);
    assertColumnValues(table.longColumn("longcol"), 12345678900L, null);
    assertColumnValues(table.doubleColumn("doublecol"), null, 13.35);
    assertColumnValues(table.booleanColumn("booleancol"), true, null);
    assertColumnValues(
        table.dateTimeColumn("datecol"), LocalDateTime.of(2019, 2, 22, 20, 54, 9), null);
    assertColumnValues(table.doubleColumn("formulacol"), null, 137.35);
  }

  @Test
  public void testSheetIndex() {
    Table table =
        new OdsReader()
            .read(OdsReadOptions.builder("../data/multiplesheets.ods").sheetIndex(1).build());
    assertNotNull(table, "No table read from multiplesheets.ods");
    assertColumnValues(table.stringColumn("stringcol"), "John", "Doe");
    assertEquals("multiplesheets.ods#Sheet2", table.name(), "table name is different");

    Table tableImplicit =
        new OdsReader().read(OdsReadOptions.builder("../data/multiplesheets.ods").build());
    // the table from the 2nd sheet should be picked up
    assertNotNull(tableImplicit, "No table read from multiplesheets.ods");


      Table tableEmptyindex = new OdsReader()
          .read(OdsReadOptions.builder("../data/multiplesheets.ods")
              .removeEmptyColumns()
              .sheetIndex(0)
              .build()
          );
      assertEquals(0, tableEmptyindex.rowCount());

    try {
      new OdsReader()
          .read(OdsReadOptions.builder("../data/multiplesheets.ods").sheetIndex(5).build());
      fail("Only 2 sheets exist, no sheet 5");
    } catch (IndexOutOfBoundsException iobe) {
      // expected
    }
  }

  @Test
  public void testCustomizedColumnTypesMixedWithDetection() {
    Table table =
        new OdsReader()
            .read(
                OdsReadOptions.builder("../data/columns.ods")
                    .columnTypesPartial(
                        ImmutableMap.of("shortcol", DOUBLE, "intcol", LONG, "formulacol", FLOAT))
                    .removeEmptyColumns()
                    .build());

    ColumnType[] columnTypes = table.typeArray();

    assertArrayEquals(
        columnTypes,
        new ColumnType[] {
            STRING,
            DOUBLE,
            LONG,
            LONG,
            DOUBLE,
            BOOLEAN,
            LOCAL_DATE_TIME,
            FLOAT,
            STRING,
            STRING,
            INTEGER
        });

    assertEquals(Lists.newArrayList("123.0", "abc", ""), table.column("mixed").asList());
    assertEquals(Lists.newArrayList("abc", "123.0", ""), table.column("mixed2").asList());
    assertEquals(Lists.newArrayList(null, 1234, 1234), table.column("intcol2").asList());
  }

  @Test
  public void testCustomizedColumnTypeAllCustomized() {
    Table table =
        new OdsReader()
            .read(
                OdsReadOptions.builder("../data/columns.ods")
                    .columnTypes(columName -> STRING)
                    .build());

    ColumnType[] columnTypes = table.typeArray();

    assertTrue(Arrays.stream(columnTypes).allMatch(columnType -> columnType.equals(STRING)));
  }

  @Test
  public void testCustomizedEmptyColumnsArePreserved() {
    Table table =
        new OdsReader()
            .read(
                OdsReadOptions.builder("../data/columns.ods")
                    .columnTypes(columName -> STRING)
                    .build());

    assertEquals(
        table.column("empty").type(),
        STRING,
        "Empty column must be preserved as it's type is specified");
  }

  @Test
  public void testCustomizedColumnStringShouldTryToPreserveValuesFromOtherOdsTypes() {
    Table table =
        new OdsReader()
            .read(
                OdsReadOptions.builder("../data/columns.ods")
                    .columnTypes(columName -> STRING)
                    .dateTimeFormat(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"))
                    .build());

    assertEquals(
        Lists.newArrayList("Hallvard", "Marit", "Quentin"),
        table.column("stringcol").asList()
    );
    assertEquals(
        Lists.newArrayList("12345678", "12345679", "12345679"),
        table.column("intcol").asList()
    );
    assertEquals(
        Lists.newArrayList("12345678900", "12345678901", "12345678901"),
        table.column("longcol").asList());
    assertEquals(
        Lists.newArrayList("12.34", "13.35", "13.35"),
        table.column("doublecol").asList()
    );
    assertEquals(
        Lists.newArrayList("true", "false", "false"),
        table.column("booleancol").asList()
    );
    assertEquals(
        Lists.newArrayList("22/02/2019 20:54:09", "23/03/2020 21:55:10", "23/03/2020 21:55:10"),
        table.column("datecol").asList()
    );
    assertEquals(
        Lists.newArrayList("135.34", "137.35", "138.35"),
        table.column("formulacol").asList()
    );
    assertEquals(Lists.newArrayList("", "", ""), table.column("empty").asList());
    assertEquals(Lists.newArrayList("123.0", "abc", ""), table.column("mixed").asList());
    assertEquals(Lists.newArrayList("abc", "123.0", ""), table.column("mixed2").asList());
    assertEquals(Lists.newArrayList("", "1234.0", "1234.0"), table.column("intcol2").asList());
  }
}
