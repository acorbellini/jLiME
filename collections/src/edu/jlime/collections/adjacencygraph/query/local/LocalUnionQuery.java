package edu.jlime.collections.adjacencygraph.query.local;

import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class LocalUnionQuery extends LocalListQuery {

	private LocalListQuery left;

	private ListQuery right;

	public LocalUnionQuery(LocalListQuery localListQuery, ListQuery query) {
		super(localListQuery.getStore());
		left = localListQuery;
		right = query;
	}

	@Override
	public int[] exec() throws Exception {
		return IntArrayUtils.union(left.query(), right.query());
	}
}