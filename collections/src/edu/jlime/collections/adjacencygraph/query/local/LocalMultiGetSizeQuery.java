package edu.jlime.collections.adjacencygraph.query.local;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.adjacencygraph.query.MultiGetSizeQuery;
import edu.jlime.collections.adjacencygraph.query.Query;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class LocalMultiGetSizeQuery implements Query<TIntIntHashMap> {

	private LocalListQuery q;

	private GetType type;

	public LocalMultiGetSizeQuery(LocalListQuery q, GetType type) {
		this.q = q;
		this.type = type;
	}

	@Override
	public TIntIntHashMap query() throws Exception {
		Logger log = Logger.getLogger(MultiGetSizeQuery.class);
		if (log.isDebugEnabled())
			log.debug("Executing MultiGet Size Query");
		TIntHashSet toSearch = new TIntHashSet();
		int[] data = q.query();
		if (type.equals(GetType.FOLLOWEES) || type.equals(GetType.NEIGHBOURS))
			toSearch.addAll(data);

		if (type.equals(GetType.FOLLOWERS) || type.equals(GetType.NEIGHBOURS))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}

		TIntIntHashMap ret = new TIntIntHashMap();
		for (int i : toSearch.toArray()) {
			int size = 0;
			if (type.equals(GetType.FOLLOWERS)
					|| type.equals(GetType.NEIGHBOURS))
				size += DataTypeUtils
						.byteArrayToIntArray(q.getStore().load(-i)).length;
			if (type.equals(GetType.FOLLOWEES)
					|| type.equals(GetType.NEIGHBOURS)) {
				size += DataTypeUtils.byteArrayToIntArray(q.getStore().load(i)).length;
			}
			ret.put(i, size);
		}
		return ret;
	}
}
