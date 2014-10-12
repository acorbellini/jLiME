package edu.jlime.collections.adjacencygraph.get;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.GraphMR;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.set.hash.TIntHashSet;

public class GetMR extends GraphMR<TIntHashSet, int[]> {

	private static final long serialVersionUID = 643643302077255726L;

	TIntHashSet res = new TIntHashSet(1000000, 0.9f);

	private GetType type;

	public GetMR(int[] data, String map, Mapper mapper, GetType type) {
		super(data, map, mapper);
		super.setDontCacheSubResults(true);
		this.type = type;
	}

	@Override
	public Map<Job<int[]>, ClientNode> map(int[] data, JobContext env)
			throws Exception {
		TIntHashSet toSearch = new TIntHashSet();
		if (type.equals(GetType.FOLLOWEES) || type.equals(GetType.NEIGHBOURS))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}
		if (type.equals(GetType.FOLLOWERS) || type.equals(GetType.NEIGHBOURS))
			toSearch.addAll(data);

		HashMap<Job<int[]>, ClientNode> res = new HashMap<>();
		Map<ClientNode, TIntArrayList> mapped = getMapper().map(
				toSearch.toArray(), env);
		for (Entry<ClientNode, TIntArrayList> e : mapped.entrySet()) {
			res.put(new GraphGet(getMapName(), e.getValue().toArray()),
					e.getKey());
		}
		return res;
	}

	@Override
	public void processSubResult(int[] subres) {
		Logger log = Logger.getLogger(GetMR.class);
		log.info("Obtained sub result on Get Map Reduce");
		synchronized (this) {
			res.addAll(subres);
		}
		log.info("Added to final result.");
	}

	@Override
	public TIntHashSet red(ArrayList<int[]> subres) throws Exception {
		Logger log = Logger.getLogger(GetMR.class);
		log.info("Finished obtaining results for Get MR");
		if (res == null)
			res = new TIntHashSet();
		return res;
	}
}