package tech.tablesaw.io.ods;

import com.github.miachm.sods.Range;
import com.github.miachm.sods.Sheet;
import com.github.miachm.sods.SpreadSheet;
import org.apache.commons.io.input.ReaderInputStream;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;

public class OdsReader implements DataReader<OdsReadOptions> {

  private static final OdsReader INSTANCE = new OdsReader();

  static {
    register(Table.defaultReaderRegistry);
  }

  public static void register(ReaderRegistry registry) {
    registry.registerExtension("ods", INSTANCE);
    registry.registerMimeType("application/vnd.oasis.opendocument.spreadsheet", INSTANCE);
    registry.registerOptions(OdsReadOptions.class, INSTANCE);
  }

  @Override
  public Table read(Source source) {
    return read(OdsReadOptions.builder(source).build());
  }

  @Override
  public Table read(OdsReadOptions options) {

    try (InputStream is = getInputStream(options)) {

      SpreadSheet spreadSheet = new SpreadSheet(is);
      int sheetIndex = options.sheetIndex == null ? 0 : options.sheetIndex;

      Sheet sheet = spreadSheet.getSheet(sheetIndex);
      int lastRow = findLastRow(sheet, 10);
      int lastColumn = findLastCol(sheet, 10);
      List<String> columnNames = new ArrayList<>(lastColumn);
      List<String[]> dataRows = new ArrayList<>(lastRow);
      readSheet(sheet, lastRow, lastColumn, columnNames, dataRows, options);
      Table table = TableBuildingUtils.build(columnNames, dataRows, options);

      if (options.removeEmptyColumns) {
        removeColumnsWithAllMissingValues(table);
      }
      return table.setName(table.name() + "#" + sheet.getName());
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private void readSheet(Sheet sheet, int lastRow, int lastColumn, List<String> columnNames, List<String[]> dataRows, ReadOptions options) {

    Coordinate startPoint = findStartPoint(sheet, lastColumn, lastRow);
    int startRow = startPoint.getRow();
    int startCol = startPoint.getCol();
    for (int colNum = startCol; colNum <= lastColumn; colNum++) {
      Object val = sheet.getRange(startRow, colNum).getValue();
      columnNames.add(val == null ? "c" + (colNum - startCol) : String.valueOf(val));
    }
    for (int rowNum = startRow + 1; rowNum <= lastRow; rowNum++) {
      String[] rowValues = new String[columnNames.size()];
      int nullCount = 0;
      for (int colNum = startCol; colNum <= lastColumn; colNum++) {
        Object val = sheet.getRange(rowNum, colNum).getValue();
        if (val == null) {
          nullCount++;
          continue;
        }
        if (val instanceof Number) {
          var bd = new BigDecimal(String.valueOf(val));
          rowValues[colNum - startCol] = bd.toPlainString();
        } else if (options.dateTimeFormatter() != null && val instanceof LocalDateTime) {
          rowValues[colNum - startCol] = options.dateTimeFormatter().format((LocalDateTime)val);
        } else if (options.dateFormatter() != null && val instanceof LocalDate) {
          rowValues[colNum - startCol] = options.dateFormatter().format((LocalDate)val);
        }
        else {
          String strVal = String.valueOf(val);
          if (strVal.isBlank()) {
            nullCount++;
          } else {
            rowValues[colNum - startCol] = strVal;
          }
        }
      }
      // Skip rows where all values are missing
      if (nullCount != lastColumn) {
        dataRows.add(rowValues);
      }
    }
  }

  private Coordinate findStartPoint(Sheet sheet, int lastColumn, int lastRow) {
    for (int row = 0; row < lastRow; row++) {
      for (int col = 0; col < lastColumn; col++) {
        Range range = sheet.getRange(row, col);
        if (range != null && range.getValue() != null) {
          return new Coordinate(row, col);
        }
      }
    }
    return new Coordinate(0,0);
  }

  private InputStream getInputStream(ReadOptions options) throws FileNotFoundException {
    if (options.source().file() != null) {
      return new FileInputStream(options.source().file());
    }
    Reader reader = options.source().reader();
    if (reader != null) {
      return new ReaderInputStream(reader, StandardCharsets.UTF_8);
    }
    return options.source().inputStream();
  }

  public List<Table> readMultiple(OdsReadOptions options) throws FileNotFoundException {

    List<Table> tables = new ArrayList<>();
    try (InputStream is = getInputStream(options)) {
      SpreadSheet spreadSheet = new SpreadSheet(is);

      for (Sheet sheet : spreadSheet.getSheets()) {
        int lastRow = findLastRow(sheet, 10);
        int lastColumn = findLastCol(sheet, 10);
        List<String> columnNames = new ArrayList<>(lastColumn);
        List<String[]> dataRows = new ArrayList<>(lastRow);
        readSheet(sheet, lastRow, lastColumn, columnNames, dataRows, options);
        Table table = TableBuildingUtils.build(columnNames, dataRows, options);
        if (options.removeEmptyColumns) {
          removeColumnsWithAllMissingValues(table);
        }
        tables.add(table);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    return tables;
  }

  private Table removeColumnsWithAllMissingValues(Table table) {
    int numRows = table.rowCount();
    List<Column<?>> columnsToRemove = new ArrayList<>();
    for (Column<?> col : table.columns()) {
      if (col.countMissing() == numRows) {
        columnsToRemove.add(col);
      }
    }
    return table.removeColumns(columnsToRemove.toArray(new Column<?>[]{}));
  }

  private int findLastRow(Sheet sheet, int numColsToScan) {
    numColsToScan = min(numColsToScan, sheet.getMaxColumns());
    int lastRow = sheet.getMaxRows() -1;
    for (int lr = lastRow; lr > 0; lr--) {
      for (int colCount = 0; colCount < numColsToScan; colCount++) {
        if (sheet.getRange(lr, colCount).getValue() != null) {
          return lr;
        }
      }
    }
    return lastRow;
  }

  protected int findLastCol(Sheet sheet, int numRowsToScan) {
    numRowsToScan = min(numRowsToScan, sheet.getMaxRows());
    var lastCol = sheet.getMaxColumns() -1; // this is often way off (too big)
    for (int lc = lastCol; lc > 0; lc--) {
      for (int rowCount = 0; rowCount < numRowsToScan; rowCount++) {
        if (sheet.getRange(rowCount, lc).getValue() != null) {
          return lc;
        }
      }
    }
    return lastCol;
  }
}
