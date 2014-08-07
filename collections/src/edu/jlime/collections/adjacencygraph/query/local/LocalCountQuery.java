package edu.jlime.collections.adjacencygraph.query.local;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.adjacencygraph.query.CountQuery;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.RemoteListQuery;
import edu.jlime.collections.adjacencygraph.query.TopQuery;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntIntHashMap;

public class LocalCountQuery extends LocalQuery<TIntIntHashMap> implements
		CountQuery {

	private RemoteListQuery remove;

	private LocalListQuery q;

	private GetType type;

	public LocalCountQuery(LocalListQuery q, GetType type) {
		this.q = q;
		this.type = type;
	}

	@Override
	public TopQuery top(int top) {
		return null;
	}

	@Override
	public TopQuery top(int top, boolean delete) {
		return null;
	}

	@Override
	public CountQuery remove(RemoteListQuery r) throws Exception {
		remove = r;
		return this;
	}

	@Override
	public ListQuery getToremove() {
		return remove;
	}

	@Override
	public TIntIntHashMap exec() throws Exception {
		int[] u = q.query();
		TIntIntHashMap count = new TIntIntHashMap();
		for (int i : u) {
			if (type.equals(GetType.FOLLOWEES)
					|| type.equals(GetType.NEIGHBOURS))
				addToCount(count,
						DataTypeUtils.byteArrayToIntArray(q.getStore().load(i)));
			if (type.equals(GetType.FOLLOWERS)
					|| type.equals(GetType.NEIGHBOURS))
				addToCount(count, DataTypeUtils.byteArrayToIntArray(q
						.getStore().load(-i)));
		}
		return count;
	}

	private void addToCount(TIntIntHashMap count, int[] byteArrayToIntArray) {
		for (int i : byteArrayToIntArray) {
			count.adjustOrPutValue(i, 1, 1);
		}

	}
}
