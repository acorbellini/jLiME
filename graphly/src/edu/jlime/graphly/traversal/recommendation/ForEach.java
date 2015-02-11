package edu.jlime.graphly.traversal.recommendation;

import edu.jlime.graphly.client.Graphly;

public interface ForEach<O> {
	O exec(long vid, Graphly tr) throws Exception;
}
