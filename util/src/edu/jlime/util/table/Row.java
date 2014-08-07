package edu.jlime.util.table;

import java.util.Iterator;

public class Row implements Dim {

	private Table t;

	private int r;

	private int i;

	public Row(Table t, int r) {
		this(t, r, t.getRowSize(r));
	}

	public Row(Table t, int r, int j) {
		this.t = t;
		this.r = r;
		this.i = j;
	}

	public void add(Cell c) {
		t.set(i++, r, c);
	}

	public void add(Dim d) {
		for (Cell cell : d)
			add(cell);
	}

	public int size() {
		return t.getRowSize(r);
	}

	public Cell get(int o1) {
		return t.get(o1, r);
	}

	@Override
	public Iterator<Cell> iterator() {
		return new Iterator<Cell>() {
			int count = 0;

			@Override
			public boolean hasNext() {
				return count < t.getRowSize(r);
			}

			@Override
			public Cell next() {
				return t.get(count++, r);
			}

			@Override
			public void remove() {
			}
		};
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (Cell c : this) {
			if (c != null)
				b.append(c.value());
			b.append(";");
		}
		return b.toString();
	}

	public void insCol(int j) {
		t.shiftRight(j, this.r);
		t.set(j, r, new ValueCell(""));
	}

}