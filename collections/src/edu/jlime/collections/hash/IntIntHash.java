package edu.jlime.collections.hash;

public interface IntIntHash extends Iterable<int[]> {

	public abstract int get(int k) throws Exception;

	public abstract void put(int i, int j);

	public abstract void putOrAdd(int i, int j);

	public abstract void remove(int k);

	public abstract int[] keys();

	public void adjustOrPutValue(int k, int c, int i);

	public void clear();

	public int size();
}