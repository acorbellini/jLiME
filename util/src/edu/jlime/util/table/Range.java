package edu.jlime.util.table;

import java.util.Iterator;

public class Range implements Iterable<Cell> {

	private Table t;

	private int c1;

	private int r1;

	private int c2;

	private int r2;

	private boolean byRow;

	public Range(Table t, int c1, int r1, int c2, int r2, boolean byRow) {
		this.t = t;
		this.c1 = c1;
		this.r1 = r1;
		this.c2 = c2;
		this.r2 = r2;

		this.byRow = byRow;
	}

	@Override
	public Iterator<Cell> iterator() {
		return new Iterator<Cell>() {
			int currentcol = c1;
			int currentrow = r1;

			@Override
			public boolean hasNext() {
				return currentcol <= c2 && currentrow <= r2;
			}

			@Override
			public Cell next() {
				Cell c = t.get(currentcol, currentrow);

				if (byRow) {
					if (currentcol >= c2) {
						currentcol = c1;
						currentrow++;
					} else
						currentcol++;
				} else {
					if (currentrow >= r2) {
						currentrow = r1;
						currentcol++;
					} else
						currentrow++;
				}
				return c;
			}

			@Override
			public void remove() {

			}
		};
	}

	public String first() {
		return t.get(c1, r1).value();
	}

	public String last() {
		return t.get(c2, r2).value();
	}
}