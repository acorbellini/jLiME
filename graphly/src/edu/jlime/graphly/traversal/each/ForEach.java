package edu.jlime.graphly.traversal.each;

import java.io.Serializable;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;

public interface ForEach<O> extends Serializable {
	O exec(long vid, GraphlyGraph tr) throws Exception;
}
