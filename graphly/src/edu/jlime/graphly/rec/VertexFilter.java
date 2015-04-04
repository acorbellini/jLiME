package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;

public interface VertexFilter {
	public boolean filter(long vid, GraphlyGraph g) throws Exception;
}
