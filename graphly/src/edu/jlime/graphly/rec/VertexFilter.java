package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.client.Graph;

public interface VertexFilter extends Serializable {
	public boolean filter(long vid, Graph g) throws Exception;
}
