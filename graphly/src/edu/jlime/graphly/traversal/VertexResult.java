package edu.jlime.graphly.traversal;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class VertexResult extends TraversalResult {

	TLongHashSet ids = new TLongHashSet();

	TLongObjectHashMap<Object> values = new TLongObjectHashMap<Object>();

	public VertexResult(long[] vids) {
		ids.addAll(vids);
	}

	public VertexResult(TLongHashSet ids) {
		this.ids.addAll(ids);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.traversal.TraversalResultI#vertices()
	 */
	@Override
	public TLongHashSet vertices() {
		TLongHashSet tLongArrayList = new TLongHashSet(ids);
		return tLongArrayList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.traversal.TraversalResultI#set(long,
	 * java.lang.Object)
	 */
	@Override
	public void set(long k, Object val) {
		values.put(k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.traversal.TraversalResultI#get(long)
	 */
	@Override
	public Object get(long k) {
		return values.get(k);
	}

	public TLongObjectHashMap<Object> getValues() {
		return values;
	}

	@Override
	public TraversalResult removeAll(TLongHashSet v) {
		TLongHashSet rem = new TLongHashSet(ids);
		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			rem.remove(it.next());
		}
		return new VertexResult(rem);
	}

	@Override
	public TraversalResult retainAll(TLongHashSet v) {
		TLongHashSet ret = new TLongHashSet(ids);
		ret.retainAll(v);
		return new VertexResult(ret);
	}

	@Override
	public String toString() {
		return ids.toString();
	}

	@Override
	public float getCount(long key) throws Exception {
		return ids.contains(key) ? 1f : 0f;
	}

	@Override
	public TLongFloatHashMap getCounts() throws Exception {
		TLongFloatHashMap ret = new TLongFloatHashMap();
		TLongIterator it = ids.iterator();
		while (it.hasNext())
			ret.put(it.next(), 1);
		return ret;
	}

}