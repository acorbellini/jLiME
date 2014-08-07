package edu.jlime.util.table;

public interface Dim extends Iterable<Cell> {

	public Cell get(int pos);

	public int size();
}