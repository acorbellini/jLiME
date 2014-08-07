package edu.jlime.collections.adjacencygraph.get;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.GraphMR;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;

public class GetMR extends GraphMR<int[], int[]> {

	private static final long serialVersionUID = 643643302077255726L;

	TIntHashSet res = new TIntHashSet();

	private GetType type;

	public GetMR(int[] data, String map, Mapper mapper, GetType type) {
		super(data, map, mapper);
		super.setDontCacheSubResults(true);
		this.type = type;
	}

	@Override
	public Map<Job<?>, JobNode> map(int[] data, JobContext env)
			throws Exception {
		TIntHashSet toSearch = new TIntHashSet();
		if (type.equals(GetType.FOLLOWERS) || type.equals(GetType.NEIGHBOURS))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}
		if (type.equals(GetType.FOLLOWEES) || type.equals(GetType.NEIGHBOURS))
			toSearch.addAll(data);

		HashMap<Job<?>, JobNode> res = new HashMap<>();
		Map<JobNode, TIntArrayList> mapped = getMapper().map(
				toSearch.toArray(), env);
		for (Entry<JobNode, TIntArrayList> e : mapped.entrySet()) {
			res.put(new GraphGet(getMapName(), e.getValue().toArray()),
					e.getKey());
		}
		return res;
	}

	@Override
	public void processSubResult(int[] subres) {
		Logger log = Logger.getLogger(GetMR.class);
		if (log.isDebugEnabled())
			log.debug("Obtained sub result on Get Map Reduce");
		synchronized (res) {
			res.addAll(subres);
		}
	}

	@Override
	public int[] red(ArrayList<int[]> subres) {
		Logger log = Logger.getLogger(GetMR.class);
		if (log.isDebugEnabled())
			log.debug("Finished obtaining results for Get MR");
		int[] ret = res.toArray();
		Arrays.sort(ret);
		return ret;
	}
}