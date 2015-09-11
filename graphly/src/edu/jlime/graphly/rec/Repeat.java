package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.client.Graph;

public interface Repeat<T> extends Serializable {
	public Object exec(T before, Graph g) throws Exception;
}
