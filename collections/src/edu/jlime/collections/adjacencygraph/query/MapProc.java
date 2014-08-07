package edu.jlime.collections.adjacencygraph.query;

import java.io.Serializable;
import java.util.Map;

public interface MapProc<T> extends Serializable {

	public Map<Integer, T> process(ListQuery user) throws Exception;
}
