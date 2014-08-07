package edu.jlime.collections.adjacencygraph.count;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Iterator;

public class LocalHashCount extends CountResult {

	private static final long serialVersionUID = -7302322790690875050L;

	TIntIntHashMap map = new TIntIntHashMap();

	public static class LocalHashCountIterator implements Iterator<int[]> {

		private TIntIntIterator map;

		public LocalHashCountIterator(TIntIntHashMap map) {
			this.map = map.iterator();
		}

		@Override
		public boolean hasNext() {
			return map.hasNext();
		}

		@Override
		public int[] next() {
			map.advance();
			return new int[] { map.key(), map.value() };
		}

		@Override
		public void remove() {
			map.remove();
		}

	}

	public Iterator<int[]> iterator() {
		return new LocalHashCountIterator(map);
	}
}