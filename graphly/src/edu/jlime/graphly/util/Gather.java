package edu.jlime.graphly.util;

import java.io.Serializable;

import edu.jlime.graphly.storenode.GraphlyStoreNode;

public interface Gather<T> extends Serializable {
	public T gather(String graph, GraphlyStoreNode node) throws Exception;
}
