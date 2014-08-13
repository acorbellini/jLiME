package edu.jlime.collections.adjacencygraph.get;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import gnu.trove.set.hash.TIntHashSet;

public class GraphGet implements Job<TIntHashSet> {

	private static final long serialVersionUID = -3316802861448545540L;

	private String map;

	private int[] data;

	public GraphGet(String map, int[] data) {
		this.map = map;
		this.data = data;
	}

	@Override
	public TIntHashSet call(JobContext ctx, JobNode peer) throws Exception {
		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(map,
				ctx);
		return dkvs.getSetOfUsers(data);
	}

}