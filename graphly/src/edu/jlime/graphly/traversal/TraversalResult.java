package edu.jlime.graphly.traversal;

import java.io.Serializable;

import gnu.trove.impl.hash.TLongLongHash;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;

public abstract class TraversalResult implements Serializable {

	public abstract TLongHashSet vertices();

	public TLongLongHash edges() {
		return new TLongLongHashMap();
	}

	public abstract TraversalResult removeAll(TLongHashSet v);

	public abstract TraversalResult retainAll(TLongHashSet v);

	public void set(long k, Object val) {

	}

	public Object get(long k) {
		return null;
	}

	public float getValue(long k) {
		return 0;
	}

	public void setValue(long k, float v) {
	}

	public TraversalResult top(int top) throws Exception {
		return this;
	}

	public abstract float getCount(long key) throws Exception;

	public abstract TLongFloatMap getCounts() throws Exception;
}