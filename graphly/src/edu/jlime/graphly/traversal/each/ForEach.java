package edu.jlime.graphly.traversal.each;

import java.io.Serializable;

import edu.jlime.graphly.client.Graphly;

public interface ForEach<O> extends Serializable {
	O exec(long vid, Graphly tr) throws Exception;
}
