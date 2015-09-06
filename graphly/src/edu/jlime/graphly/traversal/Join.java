package edu.jlime.graphly.traversal;

import java.io.Serializable;

import gnu.trove.map.hash.TLongObjectHashMap;

public interface Join<T, O> extends Serializable {
	public TLongObjectHashMap<O> join(TLongObjectHashMap<T> collected);
}
