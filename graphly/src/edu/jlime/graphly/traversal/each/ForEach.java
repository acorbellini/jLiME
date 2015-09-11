package edu.jlime.graphly.traversal.each;

import java.io.Serializable;

import edu.jlime.graphly.client.Graph;

public interface ForEach<O> extends Serializable {
	O exec(long vid, Graph tr) throws Exception;
}
