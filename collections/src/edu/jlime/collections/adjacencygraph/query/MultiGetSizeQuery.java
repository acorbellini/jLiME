package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.client.JobContext;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.log4j.Logger;

public class MultiGetSizeQuery extends CompositeQuery<int[], TIntIntHashMap> {

	private static final long serialVersionUID = -5681783873201600310L;

	private Dir type;

	public MultiGetSizeQuery(RemoteListQuery listQuery, Dir type) {
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
		if (type.equals(Dir.OUT) || type.equals(Dir.BOTH))
			toSearch.addAll(data);

		if (type.equals(Dir.IN) || type.equals(Dir.BOTH))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}

		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(
				getMapName(), c);

		TIntObjectHashMap<int[]> getRes = dkvs.get(toSearch.toArray());

		TIntIntHashMap ret = new TIntIntHashMap();
		for (int i : data) {
			int size = 0;
			if (type.equals(Dir.IN) || type.equals(Dir.BOTH))
				size += getRes.get(-i).length;
			if (type.equals(Dir.OUT) || type.equals(Dir.BOTH)) {
				size += getRes.get(i).length;
			}
			ret.put(i, size);
		}
		return ret;
	}
}
