package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.client.JobContext;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.log4j.Logger;

public class MultiGetQuery extends
		CompositeQuery<int[], TIntObjectHashMap<int[]>> {

	private static final long serialVersionUID = 1608593213488543917L;

	private GetType type;

	public MultiGetQuery(RemoteListQuery listQuery, GetType type) {
		super(listQuery);
		this.type = type;
		super.setCacheQuery(false);
	}

	@Override
	protected TIntObjectHashMap<int[]> doExec(JobContext c) throws Exception {
		Logger log = Logger.getLogger(MultiGetQuery.class);
		if (log.isDebugEnabled())
			log.debug("Executing multi get query");

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

		TIntObjectHashMap<int[]> ret = new TIntObjectHashMap<>();
		for (int i : data) {
			TIntHashSet toadd = new TIntHashSet();
			if (type.equals(GetType.FOLLOWERS)
					|| type.equals(GetType.NEIGHBOURS))
				toadd.addAll(getRes.get(-i));
			if (type.equals(GetType.FOLLOWEES)
					|| type.equals(GetType.NEIGHBOURS))
				toadd.addAll(getRes.get(i));
			ret.put(i, toadd.toArray());
		}
		if (log.isDebugEnabled())
			log.debug("Returning from multi get query");
		return ret;
	}
}
