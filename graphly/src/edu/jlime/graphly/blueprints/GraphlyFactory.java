package edu.jlime.graphly.blueprints;

import edu.jlime.graphly.client.Graphly;

public class GraphlyFactory {
	public static GremlinGraphly build(int minclients) throws Exception {
		return new GremlinGraphly(Graphly.build(minclients));
	}
}
