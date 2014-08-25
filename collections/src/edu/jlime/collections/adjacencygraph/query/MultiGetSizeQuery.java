package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.client.JobContext;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.log4j.Logger;

public class MultiGetSizeQuery extends CompositeQuery<int[], TIntIntHashMap> {

	private static final long serialVersionUID = -5681783873201600310L;

	private GetType type;

	public MultiGetSizeQuery(RemoteListQuery listQuery, GetType type) {
		super(listQuery);
		this.type = type;
		super.setCacheQuery(false);
	}

	@Override
	protected TIntIntHashMap doExec(JobContext c) throws Exception {
		Logger log = Logger.getLogger(MultiGetSizeQuery.class);
		if (log.isDebugEnabled())
			log.debug("Executing MultiGet Size Query");
		TIntHashSet toSearch = new TIntHashSet();
		int[] data = getQuery().exec(c);
		if (type.equals(GetType.FOLLOWEES) || type.equals(GetType.NEIGHBOURS))
			toSearch.addAll(data);

		if (type.equals(GetType.FOLLOWERS) || type.equals(GetType.NEIGHBOURS))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}

		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(
				getMapName(), c);

		TIntObjectHashMap<int[]> getRes = dkvs.get(toSearch.toArray());

		TIntIntHashMap ret = new TIntIntHashMap();
		for (int i : data) {
			int size = 0;
			if (type.equals(GetType.FOLLOWERS)
					|| type.equals(GetType.NEIGHBOURS))
				size += getRes.get(-i).length;
			if (type.equals(GetType.FOLLOWEES)
					|| type.equals(GetType.NEIGHBOURS)) {
				size += getRes.get(i).length;
			}
			ret.put(i, size);
		}
		return ret;
	}
}
