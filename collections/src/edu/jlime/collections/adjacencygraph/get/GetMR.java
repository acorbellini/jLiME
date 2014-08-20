package edu.jlime.collections.adjacencygraph.get;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.GraphMR;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;

public class GetMR extends GraphMR<TIntHashSet, TIntHashSet> {

	private static final long serialVersionUID = 643643302077255726L;

	TIntHashSet res = null;

	ReentrantLock lock = new ReentrantLock();

	private GetType type;

	public GetMR(int[] data, String map, Mapper mapper, GetType type) {
		super(data, map, mapper);
		super.setDontCacheSubResults(true);
		this.type = type;
	}

	@Override
	public Map<Job<TIntHashSet>, ClientNode> map(int[] data, JobContext env)
			throws Exception {
		TIntHashSet toSearch = new TIntHashSet();
		if (type.equals(GetType.FOLLOWEES) || type.equals(GetType.NEIGHBOURS))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}
		if (type.equals(GetType.FOLLOWERS) || type.equals(GetType.NEIGHBOURS))
			toSearch.addAll(data);

		HashMap<Job<TIntHashSet>, ClientNode> res = new HashMap<>();
		Map<ClientNode, TIntArrayList> mapped = getMapper().map(
				toSearch.toArray(), env);
		for (Entry<ClientNode, TIntArrayList> e : mapped.entrySet()) {
			res.put(new GraphGet(getMapName(), e.getValue().toArray()),
					e.getKey());
		}
		return res;
	}

	@Override
	public void processSubResult(TIntHashSet subres) {
		Logger log = Logger.getLogger(GetMR.class);
		// if (log.isDebugEnabled())
		log.info("Obtained sub result on Get Map Reduce");
		lock.lock();
		if (res == null)
			res = subres;
		else
			res.addAll(subres);

		lock.unlock();
		log.info("Added to final result.");
	}

	@Override
	public TIntHashSet red(ArrayList<TIntHashSet> subres) {
		Logger log = Logger.getLogger(GetMR.class);
		// if (log.isDebugEnabled())
		log.info("Finished obtaining results for Get MR");
		return res;
	}
}