package edu.jlime.graphly.traversal;

import java.io.Serializable;

public interface Step<I, O> extends Serializable {
	public O exec(I before) throws Exception;
}
