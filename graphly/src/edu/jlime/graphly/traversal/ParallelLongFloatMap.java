package edu.jlime.graphly.traversal;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import gnu.trove.TFloatCollection;
import gnu.trove.function.TFloatFunction;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.procedure.TFloatProcedure;
import gnu.trove.procedure.TLongFloatProcedure;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class ParallelLongFloatMap implements TLongFloatMap, Serializable {
	private TLongFloatHashMap[] vals = new TLongFloatHashMap[128];

	public ParallelLongFloatMap() {
		for (int i = 0; i < vals.length; i++) {
			vals[i] = new TLongFloatHashMap();
		}
	}

	private TLongFloatHashMap getMap(long k) {
		return vals[Math.abs((int) ((k * 31) % vals.length))];
	}

	@Override
	public long getNoEntryKey() {
		return vals[0].getNoEntryKey();
	}

	@Override
	public float getNoEntryValue() {
		return vals[0].getNoEntryValue();
	}

	@Override
	public float put(long key, float value) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.put(key, value);
		}
	}

	@Override
	public float putIfAbsent(long key, float value) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.putIfAbsent(key, value);
		}
	}

	@Override
	public void putAll(Map<? extends Long, ? extends Float> map) {
		for (Entry<? extends Long, ? extends Float> e : map.entrySet()) {
			put(e.getKey(), e.getValue());
		}

	}

	@Override
	public void putAll(TLongFloatMap map) {
		TLongFloatIterator it = map.iterator();
		while (it.hasNext()) {
			it.advance();
			put(it.key(), it.value());
		}
	}

	@Override
	public float get(long key) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.get(key);
		}
	}

	@Override
	public void clear() {
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				m.clear();
			}
		}
	}

	@Override
	public boolean isEmpty() {
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				if (!m.isEmpty())
					return false;
			}
		}
		return true;
	}

	@Override
	public float remove(long key) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.remove(key);
		}
	}

	@Override
	public int size() {
		int size = 0;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				size += m.size();
			}
		}
		return size;
	}

	@Override
	public TLongSet keySet() {
		TLongHashSet set = new TLongHashSet();
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				set.addAll(m.keys());
			}
		}
		return set;
	}

	@Override
	public long[] keys() {
		TLongArrayList set = new TLongArrayList();
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				set.addAll(m.keys());
			}
		}
		return set.toArray();
	}

	@Override
	public long[] keys(long[] array) {
		return null;
	}

	@Override
	public TFloatCollection valueCollection() {
		TFloatArrayList list = new TFloatArrayList();
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				list.addAll(m.valueCollection());
			}
		}
		return list;
	}

	@Override
	public float[] values() {
		return valueCollection().toArray();
	}

	@Override
	public float[] values(float[] array) {
		return null;
	}

	@Override
	public boolean containsValue(float val) {
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				if (m.containsValue(val))
					return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsKey(long key) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.containsKey(key);
		}
	}

	@Override
	public TLongFloatIterator iterator() {
		return new TLongFloatIterator() {
			int i = 0;
			TLongFloatIterator it = vals[0].iterator();

			@Override
			public void remove() {
			}

			@Override
			public boolean hasNext() {
				if (it.hasNext())
					return true;
				i++;
				while (i < vals.length) {
					it = vals[i].iterator();
					if (it.hasNext())
						return true;
					i++;
				}
				return false;
			}

			@Override
			public void advance() {
				it.advance();
			}

			@Override
			public float value() {
				return it.value();
			}

			@Override
			public float setValue(float val) {
				return 0;
			}

			@Override
			public long key() {
				return it.key();
			}
		};
	}

	@Override
	public boolean forEachKey(TLongProcedure procedure) {
		boolean res = false;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				res |= m.forEachKey(procedure);
			}
		}
		return res;
	}

	@Override
	public boolean forEachValue(TFloatProcedure procedure) {
		boolean res = false;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				res |= m.forEachValue(procedure);
			}
		}
		return res;
	}

	@Override
	public boolean forEachEntry(TLongFloatProcedure procedure) {
		boolean res = false;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				res |= m.forEachEntry(procedure);
			}
		}
		return res;
	}

	@Override
	public void transformValues(TFloatFunction function) {
		// boolean res = false;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				m.transformValues(function);
			}
		}
		// return res;
	}

	@Override
	public boolean retainEntries(TLongFloatProcedure procedure) {
		boolean res = false;
		for (TLongFloatHashMap m : vals) {
			synchronized (m) {
				res |= m.retainEntries(procedure);
			}
		}
		return res;
	}

	@Override
	public boolean increment(long key) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.increment(key);
		}
	}

	@Override
	public boolean adjustValue(long key, float amount) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.adjustValue(key, amount);
		}
	}

	@Override
	public float adjustOrPutValue(long key, float adjust_amount, float put_amount) {
		TLongFloatHashMap m = getMap(key);
		synchronized (m) {
			return m.adjustOrPutValue(key, adjust_amount, put_amount);
		}
	}

	// public void mergeWith(ParallelLongFloatMap res) {
	// for (int i = 0; i < vals.length; i++) {
	// synchronized (vals[i]) {
	// TLongFloatIterator it = res.vals[i].iterator();
	// while (it.hasNext()) {
	// it.advance();
	// vals[i].adjustOrPutValue(it.key(), it.value(), it.value());
	// }
	// }
	//
	// }
	// }

	// public void adjustOrPutValue(TLongFloatMap res) {
	// for (TLongFloatHashMap m : vals) {
	// synchronized (m) {
	// TLongFloatIterator it = res.iterator();
	// while (it.hasNext()) {
	// it.advance();
	// if (getMap(it.key()) == m)
	// m.adjustOrPutValue(it.key(), it.value(), it.value());
	// }
	// }
	// }
	// }

}
