package edu.jlime.graphly;

import edu.jlime.collections.adjacencygraph.get.GetType;

public interface GraphlyStoreNodeI {

	public abstract long[] getEdges(GetType type, Long id) throws Exception;

}