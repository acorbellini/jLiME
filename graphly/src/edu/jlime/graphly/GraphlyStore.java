package edu.jlime.graphly;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;

public interface GraphlyStore {

	public long[] getEdges(GetType type, long id);

	public Map<String, Object> getVertexAttributes(long id);

	public Map<String, Object> getEdgeAttributes(long id);
}
