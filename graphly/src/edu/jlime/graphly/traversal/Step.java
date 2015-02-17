package edu.jlime.graphly.traversal;

import java.io.Serializable;

public interface Step extends Serializable {
	public TraversalResult exec(TraversalResult before) throws Exception;
}
