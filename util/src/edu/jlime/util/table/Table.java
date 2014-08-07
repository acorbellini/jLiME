package edu.jlime.util.table;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import edu.jlime.util.table.Cell.Formatter;
import edu.jlime.util.table.Functions.CellFactory;

public class Table {

	public static Formatter DoubleFormatter = new Formatter() {

		@Override
		public String format(Cell c) {
			return String.format("%.2f", Double.valueOf(c.value()));
		}
	};

	int initsize = 1;

	int rowLimit = 0;

	int colLimit = 0;

	HashMap<Integer, HashMap<Integer, Cell>> rows = new HashMap<>();

	HashMap<Integer, HashMap<Integer, Cell>> cols = new HashMap<>();

	HashMap<String, Cell> table = new HashMap<>();

	public int getRowLimit() {
		return rowLimit;
	}

	public int getColSize(int c) {
		HashMap<Integer, Cell> col = cols.get(c);
		if (col == null || col.isEmpty())
			return 0;
		return Collections.max(col.keySet()) + 1;
	}

	public int getRowSize(int row) {
		HashMap<Integer, Cell> r = rows.get(row);
		if (r == null || r.isEmpty())
			return 0;
		return Collections.max(r.keySet()) + 1;
	}

	public int getColLimit() {
		return colLimit;
	}

	public void set(int c, int r, Cell cell) {
		table.put(c + "," + r, cell);
		HashMap<Integer, Cell> rowCellList = rows.get(r);
		if (rowCellList == null) {
			rowCellList = new HashMap<>();
			rows.put(r, rowCellList);
		}
		rowCellList.put(c, cell);

		HashMap<Integer, Cell> colCellList = cols.get(c);
		if (colCellList == null) {
			colCellList = new HashMap<>();
			cols.put(c, colCellList);
		}
		colCellList.put(r, cell);
		if (c > colLimit - 1) {
			colLimit = c + 1;
		}

		if (r > rowLimit - 1) {
			rowLimit = r + 1;
		}
	}

	@Override
	public String toString() {
		return print(";");
	}

	public String print(String sep) {
		StringBuilder builder = new StringBuilder();
		for (int j = 0; j < getRowLimit(); j++) {
			StringBuilder rowBuilder = new StringBuilder();
			for (int i = 0; i < getRowSize(j); i++) {
				if (get(i, j) == null)
					rowBuilder.append(sep + " ");
				else {
					rowBuilder.append(sep + get(i, j));
				}
			}
			builder.append(rowBuilder.substring(1) + "\n");
		}
		return builder.toString();
	}

	public static void main(String[] args) throws Exception {

		// System.out.println("adsasda ;) asdjsklajdas".replaceAll(";",
		// "\\\\;"));
		// System.out.println("/  asdasd \\".replace("\\", " "));
		// String sep = escape(";");
		// String[] split = "separado\\;va todo junto;todo junto\\;va separado"
		// .split("(?<!\\\\)" + sep);
		// System.out.println(Arrays.toString(split));

		Table t = Table
				.readCSV(
						new File(
								"C:/Users/acorbellini/Dropbox/Twitter Results/results/availablememory/13348/13348-profile-mem-availablememory-run1.csv"),
						",", ".");
		t.sortTableHeader(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = Integer.valueOf(o1.replaceAll("GridCluster", ""));
				Integer i2 = Integer.valueOf(o2.replaceAll("GridCluster", ""));
				return i1.compareTo(i2);
			}
		});

		int rowsWithData = t.getRowLimit() - 1;
		int colsWithData = t.getColLimit() - 1;

		t.fillCol("Avg", 1, rowsWithData, 1, colsWithData, Functions.AVERAGE);
		t.fillCol("StdDev", 1, rowsWithData, 1, colsWithData, Functions.STDDEV);
		t.fillCol("Sum", 1, rowsWithData, 1, colsWithData, Functions.SUM);

		t.fillRow("Max", 1, colsWithData, 1, rowsWithData, Functions.MAX);
		t.fillRow("Diff", 1, colsWithData, 1, rowsWithData, new CellFactory() {

			@Override
			public Cell build(final Range r) {
				return new FunctionCell(new Function() {
					@Override
					public String calc() {
						Double f = Double.valueOf(r.first());
						Double l = Double.valueOf(r.last());
						return new Double(l - f).toString();
					}
				});
			}
		});
		System.out.println(t);
	}

	public void fillRow(String title, int fromCol, int toCol, int i, int end,
			CellFactory cf) {
		Row r = newRow();
		r.add(new ValueCell(title));
		for (int c = fromCol; c <= toCol; c++) {
			r.add(cf.build(colRange(c, i, end)));
		}
	}

	public Range colRange(int c, int i, int end) {
		return new Range(this, c, i, c, end, true);
	}

	public void fillCol(String title, int fromRow, int toRow, int fromCol,
			int toCol, CellFactory factory) {
		Col c = newCol();
		c.add(new ValueCell(title));
		for (int r = fromRow; r <= toRow; r++)
			c.add(factory.build(rowRange(r, fromCol, toCol)));

	}

	public Range rowRange(int r, int init, int end) {
		return new Range(this, init, r, end, r, false);
	}

	public Col newCol() {
		return new Col(this, getColLimit());
	}

	public void sortTableHeader(Comparator<String> comparator) {
		sortRow(0, 1, getColLimit(), comparator);
	}

	public Integer[] getSortIndexes(final Dim r, int from, int to,
			final Comparator<String> comparator) {
		Integer[] indexes = new Integer[r.size()];
		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = i;
		}
		Arrays.sort(indexes, from, to, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				String v1 = "";
				String v2 = "";
				if (r.get(o1) != null)
					v1 = r.get(o1).value();

				if (r.get(o2) != null)
					v2 = r.get(o2).value();

				return comparator.compare(v1, v2);
			}
		});
		return indexes;
	}

	private void sortCol(int col, int from, int to,
			Comparator<String> comparator) {
		Col column = getCol(col);
		Integer[] indexes = getSortIndexes(column, from, to, comparator);
		reorderRows(indexes);
	}

	private Col getCol(int col) {
		return new Col(this, col);
	}

	private void reorderRows(Integer[] rowIndex) {
		for (int i = 0; i < getColLimit(); i++) {
			HashMap<Integer, Cell> aux = new HashMap<>();
			for (int j = 0; j < getRowLimit(); j++)
				aux.put(j, get(i, j));

			for (int j = 0; j < rowIndex.length; j++) {
				int next = rowIndex[i];
				set(i, j, aux.get(next));
			}
		}
	}

	private void sortRow(int row, int from, int to,
			Comparator<String> comparator) {
		Row copy = getRow(row);
		Integer[] indexes = getSortIndexes(copy, from, to, comparator);
		reorderColumns(indexes);
	}

	private void reorderColumns(Integer[] colIndex) {
		for (int i = 0; i < getRowLimit(); i++) {
			HashMap<Integer, Cell> aux = new HashMap<>();
			for (int j = 0; j < getColLimit(); j++)
				aux.put(j, get(j, i));

			for (int j = 0; j < colIndex.length; j++) {
				int next = colIndex[j];
				set(j, i, aux.get(next));
			}
		}
	}

	public Cell get(int col, int row) {
		return table.get(col + "," + row);
	}

	public static Table readCSV(File csv, String columnSep, String decimalSep)
			throws Exception {
		CSVBuilder builder = new CSVBuilder(csv);
		builder.setColumnSep(columnSep);
		builder.setDecimalSep(decimalSep);
		return builder.toTable();
	}

	public Row newRow() {
		return insRow(getRowLimit());
	}

	public Row getRow(String string) {
		for (int i = 0; i < getColSize(0); i++) {
			if (get(0, i) != null && get(0, i).value().equals(string))
				return getRow(i);
		}
		return null;
	}

	public Row getRow(int row) {
		return new Row(this, row);
	}

	public void sortRowHeader(Comparator comparator) {
		sortCols(0, 1, getRowLimit(), comparator);
	}

	private void sortCols(int col, int init, int end, Comparator comparator) {
		Col copy = getCol(col);
		Integer[] indexes = getSortIndexes(copy, init, end, comparator);
		reorderRows(indexes);
	}

	public void sortCols(int col, int from, int to) {
		sortCols(col, from, to, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
	}

	public void delRow(int row) {
		for (int c = 0; c < getRowSize(row); c++) {
			del(c, row);
		}

		for (int r = row; r < getRowLimit(); r++) {
			for (int c = 0; c < getRowSize(r + 1); c++) {
				set(c, r, get(c, r + 1));
				del(c, r + 1);
			}
		}
		updateMaxRow();
	}

	private void del(int c, int r) {
		table.remove(c + "," + r);

		HashMap<Integer, Cell> col = cols.get(c);
		if (col != null) {
			col.remove(r);
			if (col.isEmpty())
				cols.remove(c);
		}

		HashMap<Integer, Cell> row = rows.get(r);
		if (row != null) {
			row.remove(c);
			if (row.isEmpty())
				rows.remove(r);
		}

	}

	private void updateMaxRow() {
		rowLimit = -1;
		for (Integer c : cols.keySet()) {
			int colSize = getColSize(c);
			if (colSize > rowLimit)
				rowLimit = colSize;
		}
	}

	public void delCol(int col) {
		for (int c = col; c < getColLimit(); c++) {
			for (int r = 0; r < getRowLimit(); r++) {
				set(c, r, get(c + 1, r));
				del(getRowSize(r) - 1, r);
			}
		}
		updateMaxCol();
	}

	private void updateMaxCol() {
		colLimit = -1;
		for (Integer r : rows.keySet()) {
			int rowSize = getRowSize(r);
			if (rowSize > colLimit)
				colLimit = rowSize;
		}
	}

	public Row insRow(int row) {
		for (int r = getRowLimit(); r > row; r--) {
			for (int c = 0; c < getColLimit(); c++) {
				set(c, r, get(c, r - 1));
				set(c, row, null);
			}
		}
		updateMaxRow();
		return new Row(this, row, 0);
	}

	public Col insCol(int col) {
		for (int c = getColLimit(); c > col; c--) {
			shiftRight(c, 0);
		}
		for (int r = 0; r < getRowLimit(); r++) {
			set(col, r, new ValueCell(""));
		}

		updateMaxCol();
		return new Col(this, col, 0);

	}

	void shiftRight(int c, int rInit) {
		for (int r = rInit; r < getRowLimit(); r++) {
			set(c, r, get(c - 1, r));
		}
	}

	public Col getCol(String title) {
		for (int i = 0; i < getRowSize(0); i++) {
			if (get(i, 0) != null && get(i, 0).value().equals(title))
				return getCol(i);
		}
		return null;
	}

	public boolean isEmpty() {
		return rowLimit == 0;
	}

	public void merge(int c, int r, int c2, int r2) {
		set(c, r, new ValueCell(get(c, r).value() + get(c2, r2).value()));
		shiftLeft(c2, r2);
	}

	private void shiftLeft(int c, int r) {
		for (int i = c; i < getRowSize(r); i++) {
			set(i, r, get(i + 1, r));
		}
		del(getRowSize(r) - 1, r);
	}

	public void mergeRows(int into, int from) {
		Row r = getRow(into);
		for (Cell c : getRow(from)) {
			r.add(c);
		}
		delRow(from);
	}
}
