package edu.jlime.collections.adjacencygraph;

import edu.jlime.collections.adjacencygraph.query.ListQuery;

public interface AdjacencyGraph {

	public abstract ListQuery getUser(int id);

	public abstract ListQuery get(int[] ids);

	public abstract void close() throws Exception;

}