package edu.jlime.graphly.traversal;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class ValueResult extends TraversalResult {
	Float res;

	public ValueResult(Float res) {
		this.res = res;
	}

	public Float getRes() {
		return res;
	}

	@Override
	public TLongHashSet vertices() {
		return null;
	}

	@Override
	public TraversalResult removeAll(TLongHashSet v) {
		return null;
	}

	@Override
	public TraversalResult retainAll(TLongHashSet v) {
		return null;
	}

	@Override
	public float getCount(long key) {
		return 0;
	}

	@Override
	public TLongFloatHashMap getCounts() {
		return null;
	}

	@Override
	public String toString() {
		return getRes().toString();
	}
}
