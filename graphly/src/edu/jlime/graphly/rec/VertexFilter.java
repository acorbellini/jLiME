package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.client.GraphlyGraph;

public interface VertexFilter extends Serializable {
	public boolean filter(long vid, GraphlyGraph g) throws Exception;
}
