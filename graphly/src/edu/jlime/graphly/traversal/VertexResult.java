package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Arrays;

public class VertexResult extends TraversalResult {

	long[] ids = new long[] {};

	TLongObjectHashMap<Object> values = new TLongObjectHashMap<Object>();

	public VertexResult(TLongHashSet rem) {
		this.ids = rem.toArray();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.traversal.TraversalResultI#vertices()
	 */
	@Override
	public TLongArrayList vertices() {
		return new TLongArrayList(ids);
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
	public TraversalResult removeAll(TLongArrayList v) {
		TLongHashSet rem = new TLongHashSet(ids);
		rem.removeAll(v);
		return new VertexResult(rem);
	}

	@Override
	public TraversalResult retainAll(TLongArrayList v) {
		TLongHashSet ret = new TLongHashSet(ids);
		ret.removeAll(v);
		return new VertexResult(ret);
	}

	@Override
	public String toString() {
		return Arrays.toString(ids);
	}

}