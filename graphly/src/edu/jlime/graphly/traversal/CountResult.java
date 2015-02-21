package edu.jlime.graphly.traversal;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongFloatProcedure;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

public class CountResult extends TraversalResult {

	private TLongFloatHashMap vals;

	public CountResult(TLongFloatHashMap intvals) {
		this.vals = intvals;
	}

	public CountResult(Map<Long, Integer> ret) {
		this.vals = new TLongFloatHashMap();
		for (Entry<Long, Integer> e : ret.entrySet()) {
			vals.put(e.getKey(), e.getValue());
		}

	}

	public CountResult(TLongObjectHashMap<Object> res) {
		this.vals = new TLongFloatHashMap();
		TLongObjectIterator<Object> it = res.iterator();
		while (it.hasNext()) {
			it.advance();
			vals.put(it.key(), (Float) it.value());
		}
	}

	@Override
	public TLongArrayList vertices() {
		return TLongArrayList.wrap(vals.keys());
	}

	@Override
	public float getValue(long k) {
		return vals.get(k);
	}

	@Override
	public TraversalResult removeAll(final TLongArrayList v) {
		vals.retainEntries(new TLongFloatProcedure() {

			@Override
			public boolean execute(long k, float val) {
				return !v.contains(k);
			}
		});
		return new CountResult(vals);
	}

	@Override
	public TraversalResult retainAll(final TLongArrayList v) {
		vals.retainEntries(new TLongFloatProcedure() {

			@Override
			public boolean execute(long k, float val) {
				return v.contains(k);
			}
		});
		return new CountResult(vals);
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		TreeSet<Long> res = new TreeSet<>(new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				int comp = Float.compare(vals.get(o1), vals.get(o2)) * -1;
				if (comp == 0)
					return o1.compareTo(o2);
				return comp;
			}
		});
		TLongFloatIterator it = vals.iterator();
		while (it.hasNext()) {
			it.advance();
			res.add(it.key());
		}

		for (Long l : res) {
			ret.append(l + "=" + vals.get(l) + "\n");
		}

		return ret.toString();
	}

	@Override
	public TraversalResult top(int top) {
		TreeSet<Long> finalRes = new TreeSet<Long>(new Comparator<Long>() {

			@Override
			public int compare(Long o1, Long o2) {
				int comp = Float.compare(vals.get(o1), vals.get(o2));
				if (comp == 0)
					return o1.compareTo(o2);
				return comp;
			}
		});
		TLongFloatIterator it = vals.iterator();
		while (it.hasNext()) {
			it.advance();
			long k = it.key();
			float v = it.value();
			if (finalRes.size() < top)
				finalRes.add(k);
			else {
				if (v > vals.get(finalRes.first())) {
					finalRes.remove(finalRes.first());
					finalRes.add(k);
				}
			}
		}
		TLongFloatHashMap res = new TLongFloatHashMap();
		for (Long k : finalRes) {
			res.put(k, vals.get(k));
		}
		return new CountResult(res);
	}
}
