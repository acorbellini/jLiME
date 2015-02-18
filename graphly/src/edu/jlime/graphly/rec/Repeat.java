package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.client.Graphly;

public interface Repeat<T> extends Serializable {
	public Object exec(T before, Graphly g) throws Exception;
}
