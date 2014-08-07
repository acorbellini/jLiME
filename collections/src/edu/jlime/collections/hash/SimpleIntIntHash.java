package edu.jlime.collections.hash;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.Serializable;
import java.util.Iterator;

public class SimpleIntIntHash implements IntIntHash, Serializable {

	private static final long serialVersionUID = 7203062209610429088L;

	public static class SimpleIntIntHashIterator implements Iterator<int[]>,
			Serializable {

		private static final long serialVersionUID = 4129718695023311899L;

		private TIntIntIterator it;

		public SimpleIntIntHashIterator(TIntIntHashMap hash) {
			this.it = hash.iterator();

		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public int[] next() {
			it.advance();
			return new int[] { it.key(), it.value() };
		}

		@Override
		public void remove() {
			it.remove();
		}
	}

	TIntIntHashMap hash = new TIntIntHashMap();

	@Override
	public int get(int k) throws Exception {
		return hash.get(k);
	}

	@Override
	public void put(int i, int j) {
		hash.put(i, j);
	}

	@Override
	public void putOrAdd(int i, int j) {
		hash.adjustOrPutValue(i, j, j);
	}

	@Override
	public void remove(int k) {
		hash.remove(k);
	}

	@Override
	public int[] keys() {
		return hash.keys();
	}

	@Override
	public synchronized void adjustOrPutValue(int k, int c, int i) {
		hash.adjustOrPutValue(k, c, i);

	}

	@Override
	public Iterator<int[]> iterator() {
		return new SimpleIntIntHashIterator(hash);
	}

	@Override
	public void clear() {
		hash.clear();
	}

	@Override
	public int size() {
		return hash.size();
	}

	public void adjustOrPutValue(TIntIntHashMap map) {
		int[] keys = map.keys();
		for (int k : keys) {
			int v = map.get(k);
			adjustOrPutValue(k, v, v);
		}
	}

}
