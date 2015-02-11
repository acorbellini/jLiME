package edu.jlime.graphly;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;

public interface GraphlyStore {

	public long[] getEdges(Dir type, long id);

	public Map<String, Object> getVertexAttributes(long id);

	public Map<String, Object> getEdgeAttributes(long id);
}
