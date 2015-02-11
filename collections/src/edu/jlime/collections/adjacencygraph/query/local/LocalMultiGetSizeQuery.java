package edu.jlime.collections.adjacencygraph.query.local;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.collections.adjacencygraph.query.MultiGetSizeQuery;
import edu.jlime.collections.adjacencygraph.query.Query;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class LocalMultiGetSizeQuery implements Query<TIntIntHashMap> {

	private LocalListQuery q;

	private Dir type;

	public LocalMultiGetSizeQuery(LocalListQuery q, Dir type) {
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
		if (type.equals(Dir.OUT) || type.equals(Dir.BOTH))
			toSearch.addAll(data);

		if (type.equals(Dir.IN) || type.equals(Dir.BOTH))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}

		TIntIntHashMap ret = new TIntIntHashMap();
		for (int i : toSearch.toArray()) {
			int size = 0;
			if (type.equals(Dir.IN)
					|| type.equals(Dir.BOTH))
				size += DataTypeUtils
						.byteArrayToIntArray(q.getStore().load(-i)).length;
			if (type.equals(Dir.OUT)
					|| type.equals(Dir.BOTH)) {
				size += DataTypeUtils.byteArrayToIntArray(q.getStore().load(i)).length;
			}
			ret.put(i, size);
		}
		return ret;
	}
}
