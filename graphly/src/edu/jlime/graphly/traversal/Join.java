package edu.jlime.graphly.traversal;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;

public interface Join<T, O> extends Serializable {
	public TLongObjectHashMap<O> join(TLongObjectHashMap<T> collected);
}
