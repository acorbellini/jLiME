package edu.jlime.collections.adjacencygraph.get;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.GraphMR;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;

public class GetMR extends GraphMR<TIntHashSet, int[]> {

	private static final long serialVersionUID = 643643302077255726L;

	TIntHashSet res = new TIntHashSet(1000000, 0.9f);

	private Dir type;

	public GetMR(int[] data, String map, Mapper mapper, Dir type) {
		super(data, map, mapper);
		super.setDontCacheSubResults(true);
		this.type = type;
	}

	@Override
	public Map<Job<int[]>, ClientNode> map(int[] data, JobContext env)
			throws Exception {
		TIntHashSet toSearch = new TIntHashSet();
		if (type.equals(Dir.OUT)
				|| type.equals(Dir.BOTH))
			for (int i = 0; i < data.length; i++) {
				toSearch.add(-1 * data[i]);
			}
		if (type.equals(Dir.IN)
				|| type.equals(Dir.BOTH))
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
	public boolean processSubResult(int[] subres) {
		Logger log = Logger.getLogger(GetMR.class);
		log.info("Obtained sub result on Get Map Reduce");
		synchronized (this) {
			res.addAll(subres);
		}
		log.info("Added to final result.");
		return true;
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