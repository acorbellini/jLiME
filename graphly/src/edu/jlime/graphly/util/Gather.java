package edu.jlime.graphly.util;

import java.io.Serializable;

import edu.jlime.graphly.storenode.StoreNodeImpl;

public interface Gather<T> extends Serializable {
	public T gather(String graph, StoreNodeImpl node) throws Exception;
}
