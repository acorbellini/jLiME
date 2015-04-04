package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.client.GraphlyGraph;

public interface Repeat<T> extends Serializable {
	public Object exec(T before, GraphlyGraph g) throws Exception;
}
