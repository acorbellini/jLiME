package edu.jlime.collections.adjacencygraph.query.local;

import java.util.Arrays;

import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.intintarray.db.Store;
import gnu.trove.list.array.TIntArrayList;

public class LocalUserQuery extends LocalListQuery {

	public LocalUserQuery(Store store, int[] users) {
		super(store);
		this.users = users;
	}

	public LocalUserQuery(Store store, int i) {
		this(store, new int[] { i });
	}

	int[] users;

	private ListQuery toRem;

	private ListQuery filter;

	@Override
	public ListQuery remove(ListQuery toRem) {
		this.toRem = toRem;
		return this;
	}

	@Override
	public ListQuery getToRemove() {
		return toRem;
	}

	@Override
	public ListQuery filterBy(ListQuery filter) {
		this.filter = filter;
		return this;
	}

	@Override
	public ListQuery getFilter() {
		return filter;
	}

	@Override
	public int[] exec() throws Exception {
		TIntArrayList list = new TIntArrayList(users);
		if (toRem != null) {
			int[] r = toRem.query();
			list.removeAll(r);
		}
		if (filter != null) {
			int[] f = filter.query();
			list.retainAll(f);
		}
		return list.toArray();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LocalUserQuery))
			return false;
		LocalUserQuery other = (LocalUserQuery) obj;

		return (toRem == null || other.toRem.equals(toRem))
				&& (filter == null || other.filter.equals(filter))
				&& Arrays.equals(users, other.users);
	}
}
