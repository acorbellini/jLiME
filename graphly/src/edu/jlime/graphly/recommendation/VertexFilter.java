package edu.jlime.graphly.recommendation;

import edu.jlime.graphly.client.Graphly;

public interface VertexFilter {
	public boolean filter(long vid, Graphly g) throws Exception;
}
