package edu.jlime.collections.adjacencygraph.query.local;

import java.util.Arrays;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.set.hash.TIntHashSet;

public class LocalGetQuery extends LocalListQuery {

	private Dir type;

	private LocalListQuery q;

	private ListQuery toRem;

	private ListQuery filter;

	public LocalGetQuery(LocalListQuery q, Dir type) {
		super(q.getStore());
		this.q = q;
		this.type = type;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LocalGetQuery))
			return false;
		LocalGetQuery other = (LocalGetQuery) obj;

		return other.type.equals(this.type)
				&& (toRem == null || other.toRem.equals(toRem))
				&& (filter == null || other.filter.equals(filter))
				&& q.equals(other.q);
	}

	@Override
	public int[] exec() throws Exception {
		int[] users = q.query();
		TIntHashSet list = new TIntHashSet(50000);
		for (int i : users) {
			if (type.equals(Dir.OUT)
					|| type.equals(Dir.BOTH)) {
				byte[] load = getStore().load(i);
				if (load != null)
					list.addAll(DataTypeUtils.byteArrayToIntArray(load));
			}
			if (type.equals(Dir.IN)
					|| type.equals(Dir.BOTH)) {
				byte[] load = getStore().load(-i);
				if (load != null)
					list.addAll(DataTypeUtils.byteArrayToIntArray(load));
			}
		}
		int[] ret = list.toArray();
		Arrays.sort(ret);
		return ret;
	}

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
}
