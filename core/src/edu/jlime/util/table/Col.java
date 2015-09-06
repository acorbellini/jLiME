package edu.jlime.util.table;

import java.util.Iterator;

public class Col implements Dim {

	private Table t;

	private int c;

	private int i;

	public Col(Table t, int c) {
		this(t, c, t.getColSize(c));
	}

	public Col(Table t, int c, int j) {
		this.t = t;
		this.c = c;
		this.i = j;
	}

	public void add(Cell cell) {
		t.set(c, i++, cell);
	}

	public void add(Cell[] max) {
		for (Cell cell : max) {
			add(cell);
		}
	}

	@Override
	public Cell get(int pos) {
		return t.get(c, pos);
	}

	@Override
	public int size() {
		return t.getColSize(c);
	}

	@Override
	public Iterator<Cell> iterator() {
		return new Iterator<Cell>() {
			int count = 0;

			@Override
			public boolean hasNext() {
				return count < t.getColSize(c);
			}

			@Override
			public Cell next() {
				return t.get(c, count++);
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
			b.append("\n");
		}
		return b.toString();
	}

	public int column() {
		return c;
	}

	public Cell find(String string) {
		for (Cell c : this) {
			if (string.equals(c.value()))
				return c;
		}
		return null;
	}
}