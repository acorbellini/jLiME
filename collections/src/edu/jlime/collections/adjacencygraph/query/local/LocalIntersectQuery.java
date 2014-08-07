package edu.jlime.collections.adjacencygraph.query.local;

import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class LocalIntersectQuery extends LocalListQuery {

	private LocalListQuery left;

	private ListQuery right;

	public LocalIntersectQuery(LocalListQuery localListQuery, ListQuery query) {
		super(localListQuery.getStore());
		left = localListQuery;
		right = query;
	}

	@Override
	public int[] exec() throws Exception {
		return IntArrayUtils.intersectArrays(left.query(), right.query());
	}
}
