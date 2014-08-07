package edu.jlime.collections.adjacencygraph;

import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.local.LocalUserQuery;
import edu.jlime.collections.intintarray.db.Store;

public class LocalAdjacencyGraph implements AdjacencyGraph {

	private Store store;

	public LocalAdjacencyGraph(Store store) {
		this.store = store;
	}

	@Override
	public ListQuery getUser(int id) {
		return get(new int[] { id });
	}

	@Override
	public ListQuery get(int[] ids) {
		return new LocalUserQuery(store, ids);
	}

	@Override
	public void close() {
	}

}
