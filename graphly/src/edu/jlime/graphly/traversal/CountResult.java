package edu.jlime.graphly.traversal;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.procedure.TLongFloatProcedure;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CountResult extends TraversalResult {
	TLongFloatMap map;

	public CountResult(TLongFloatMap map) {
		this.map = map;
	}

	@Override
	public TLongHashSet vertices() {
		return new TLongHashSet(map.keys());
	}

	@Override
	public float getValue(long k) {
		return map.get(k);
	}

	@Override
	public TraversalResult removeAll(final TLongHashSet v) {

		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			long next = it.next();
			map.remove(next);
		}

		// final int threads = Runtime.getRuntime().availableProcessors();
		// ExecutorService exec = Executors.newFixedThreadPool(threads);
		// for (int i = 0; i < threads; i++) {
		// final int tID = i;
		// exec.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		// TLongIterator it = v.iterator();
		// while (it.hasNext()) {
		// long next = it.next();
		// if (next % threads == tID)
		// map.remove(next);
		// }
		//
		// }
		// });
		// }
		// exec.shutdown();
		// try {
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		return this;
	}

	@Override
	public TraversalResult retainAll(final TLongHashSet v) {
		map.retainEntries(new TLongFloatProcedure() {

			@Override
			public boolean execute(long k, float val) {
				return v.contains(k);
			}
		});

		return this;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		TreeSet<Long> res = new TreeSet<>(new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				int comp = Float.compare(map.get(o1), map.get(o2)) * -1;
				if (comp == 0)
					return o1.compareTo(o2);
				return comp;
			}
		});
		TLongFloatIterator it = map.iterator();
		while (it.hasNext()) {
			it.advance();
			res.add(it.key());
		}

		for (Long l : res) {
			ret.append(l + "=" + map.get(l) + "\n");
		}
		return ret.toString();
	}

	@Override
	public TraversalResult top(int top) {
		TreeSet<Long> finalRes = new TreeSet<Long>(new Comparator<Long>() {

			@Override
			public int compare(Long o1, Long o2) {
				int comp = Float.compare(map.get(o1), map.get(o2));
				if (comp == 0)
					return o1.compareTo(o2);
				return comp;
			}
		});
		TLongFloatIterator it = map.iterator();
		while (it.hasNext()) {
			it.advance();
			long k = it.key();
			float v = it.value();
			if (finalRes.size() < top)
				finalRes.add(k);
			else {
				if (v > map.get(finalRes.first())) {
					finalRes.remove(finalRes.first());
					finalRes.add(k);
				}
			}
		}
		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Long k : finalRes) {
			ret.put(k, map.get(k));
		}
		return new CountResult(ret);
	}

	public void put(Long k, float f) {
		map.put(k, f);
	}

	@Override
	public float getCount(long key) {
		return map.get(key);
	}

	@Override
	public TLongFloatMap getCounts() {
		return map;
	}

	public void adjustOrPutValue(long key, float value) {
		map.adjustOrPutValue(key, value, value);
	}

	public int size() {
		return map.size();
	}

	public TLongFloatIterator iterator() {
		return map.iterator();
	}
}
