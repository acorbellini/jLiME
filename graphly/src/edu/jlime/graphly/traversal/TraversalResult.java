package edu.jlime.graphly.traversal;

import gnu.trove.impl.hash.TLongLongHash;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

import java.io.Serializable;

public interface TraversalResult extends Serializable {

	public abstract TLongArrayList vertices();

	public default TLongLongHash edges() {
		return new TLongLongHashMap();
	}

	public TraversalResult removeAll(TLongArrayList v);

	public TraversalResult retainAll(TLongArrayList v);

	public default void set(long k, Object val) {

	}

	public default Object get(long k) {
		return null;
	}

	public default int getInt(long k) {
		return 0;
	}

	public default void setInt(long k, int v) {
	}

}