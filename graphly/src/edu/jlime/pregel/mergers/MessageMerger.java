package edu.jlime.pregel.mergers;

import java.io.Serializable;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

public interface MessageMerger extends Serializable {

	void merge(long to, float curr, TLongFloatHashMap map);

	<T> void merge(T type, float val, TObjectFloatHashMap<T> broadcast);

}
