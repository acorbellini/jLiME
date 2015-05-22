package edu.jlime.graphly.traversal;

import gnu.trove.impl.hash.TLongLongHash;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

import java.io.Serializable;

public abstract class TraversalResult implements Serializable {

	public abstract TLongArrayList vertices();

	public TLongLongHash edges() {
		return new TLongLongHashMap();
	}

	public abstract TraversalResult removeAll(TLongArrayList v);

	public abstract TraversalResult retainAll(TLongArrayList v);

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

	public TraversalResult top(int top) {
		return this;
	}
}