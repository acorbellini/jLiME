package edu.jlime.collections.hash;

import gnu.trove.list.array.TIntArrayList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class TrivialIntIntHash implements Iterable<int[]>, Serializable,
		IntIntHash {

	private static final long serialVersionUID = 1790634100863600451L;

	public static class SimpleIntIntHashIterator implements Iterator<int[]>,
			Serializable {

		private static final long serialVersionUID = -2243133964952706248L;

		private TrivialIntIntHash hash;

		private int pos = 0;

		private int posBucket = 0;

		public SimpleIntIntHashIterator(TrivialIntIntHash cheapHash) {
			this.hash = cheapHash;

		}

		@Override
		public boolean hasNext() {
			while (pos < hash.MAX_BUCKETS
					&& (hash.buckets[pos] == null || posBucket == hash.sizes[pos])) {
				pos++;
				posBucket = 0;
			}

			if (pos >= hash.MAX_BUCKETS)
				return false;
			return true;
		}

		@Override
		public int[] next() {
			int[] res = new int[] { hash.buckets[pos][posBucket],
					hash.bucketsValues[pos][posBucket] };
			posBucket++;
			return res;
		}

		@Override
		public void remove() {
		}
	}

	int[][] buckets;

	int[][] bucketsValues;

	Integer[] sizes;

	int MAX_BUCKETS = 5000;

	private Integer size = 0;

	public TrivialIntIntHash() {
		clear();
	}

	public void adjustOrPutValue(int k, int c, int i) {
		put(k, c, i, true);
	}

	@Override
	public int get(int k) throws Exception {
		int pos = hash(k);
		int kpos = Arrays.binarySearch(buckets[pos], 0, sizes[pos], k);
		if (kpos > -1)
			return bucketsValues[pos][kpos];
		// Deberia cambiarse por return null, esta la excepcion con fines de
		// debug
		throw new Exception("Key was not found.");
	}

	public int getMaxBuckets() {
		return MAX_BUCKETS;
	}

	public int hash(int k) {
		return (int) Math.abs((k * 5700357409661598721L) % buckets.length);
	}

	@Override
	public Iterator<int[]> iterator() {
		return new SimpleIntIntHashIterator(this);
	}

	@Override
	public void put(int i, int j) {
		put(i, j, j, false);
	}

	public void put(int k, int c, int i, boolean adjust) {
		int pos = hash(k);
		synchronized (sizes[pos]) {
			if (sizes[pos] > 0) {
				int kpos = Arrays.binarySearch(buckets[pos], 0, sizes[pos], k);
				if (kpos > -1) {
					if (adjust)
						bucketsValues[pos][kpos] += c;
					else
						bucketsValues[pos][kpos] = c;
					return;
				}
				synchronized (size) {
					size++;
				}
				kpos = -(kpos + 1);

				if (sizes[pos] == buckets[pos].length) {
					int newSize = buckets[pos].length * 2;
					buckets[pos] = Arrays.copyOf(buckets[pos], newSize);
					bucketsValues[pos] = Arrays.copyOf(bucketsValues[pos],
							newSize);
				}
				if (kpos < sizes[pos]) {
					System.arraycopy(buckets[pos], kpos, buckets[pos],
							kpos + 1, sizes[pos] - kpos);
					System.arraycopy(bucketsValues[pos], kpos,
							bucketsValues[pos], kpos + 1, sizes[pos] - kpos);
				}
				buckets[pos][kpos] = k;
				bucketsValues[pos][kpos] = i;
			} else {
				buckets[pos][0] = k;
				bucketsValues[pos][0] = i;
			}
			sizes[pos]++;
		}
	}

	@Override
	public void putOrAdd(int i, int j) {
		adjustOrPutValue(i, j, j);
	}

	@Override
	public void remove(int k) {
		int pos = hash(k);
		synchronized (sizes[pos]) {
			int kpos = Arrays.binarySearch(buckets[pos], 0, sizes[pos], k);
			if (kpos > -1) {
				System.arraycopy(buckets[pos], kpos + 1, buckets[pos], kpos,
						sizes[pos] - kpos - 1);
				System.arraycopy(bucketsValues[pos], kpos + 1,
						bucketsValues[pos], kpos, sizes[pos] - kpos - 1);
				sizes[pos]--;
			}

		}
		synchronized (size) {
			size--;
		}

	}

	@Override
	public int[] keys() {
		TIntArrayList list = new TIntArrayList();
		for (int bucketsPos = 0; bucketsPos < buckets.length; bucketsPos++) {
			int[] bucket = buckets[bucketsPos];
			synchronized (sizes[bucketsPos]) {
				for (int innerBucket = 0; innerBucket < sizes[bucketsPos]; innerBucket++)
					list.add(bucket[innerBucket]);
			}
		}
		return list.toArray();
	}

	@Override
	public void clear() {
		buckets = new int[MAX_BUCKETS][100];
		bucketsValues = new int[MAX_BUCKETS][100];
		sizes = new Integer[MAX_BUCKETS];
		Arrays.fill(sizes, 0);
		size = 0;
	}

	@Override
	public int size() {
		return size;
	}
}
