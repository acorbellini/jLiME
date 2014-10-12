package edu.jlime.collections.adjacencygraph.get;

import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class GraphGet implements Job<int[]> {

	private static final long serialVersionUID = -3316802861448545540L;

	private String map;

	private int[] data;

	public GraphGet(String map, int[] data) {
		this.map = map;
		this.data = data;
	}

	@Override
	public int[] call(JobContext ctx, ClientNode peer) throws Exception {
		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(map,
				ctx);
		return dkvs.getSetOfUsers(data);
	}
}