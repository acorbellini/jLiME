package edu.jlime.collections.adjacencygraph.query;

import java.io.Serializable;

public interface ForEachQueryProc<T> extends Serializable {

	public T call(ListQuery userId) throws Exception;
}
