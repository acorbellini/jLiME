package edu.jlime.collections.adjacencygraph.query.local;

import edu.jlime.collections.adjacencygraph.query.Query;

public class LocalSizeQuery implements Query<Integer> {

	private LocalListQuery q;

	public LocalSizeQuery(LocalListQuery q) {
		this.q = q;
	}

	@Override
	public Integer query() throws Exception {
		return q.query().length;
	}

}
