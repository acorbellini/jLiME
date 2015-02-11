package edu.jlime.graphly.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class VertexForkJoin extends MapReduceTask<long[], TLongHashSet, long[]> {

	private static final long serialVersionUID = 643643302077255726L;

	TLongHashSet res = new TLongHashSet();

	private Mapper mapper;

	private Dir dir;

	public VertexForkJoin(long[] data, Dir dir, Mapper mapper) {
		super(data);
		super.setDontCacheSubResults(true);
		this.mapper = mapper;
		this.dir = dir;
	}

	@Override
	public Map<Job<long[]>, ClientNode> map(long[] data, JobContext env)
			throws Exception {
		HashMap<Job<long[]>, ClientNode> res = new HashMap<>();

		Map<ClientNode, TLongArrayList> mapped = mapper.map(data, env);

		for (Entry<ClientNode, TLongArrayList> e : mapped.entrySet()) {
			res.put(new VertexForkJoinJob(dir, e.getValue().toArray()),
					e.getKey());
		}
		return res;
	}

	@Override
	public boolean processSubResult(long[] subres) {
		Logger log = Logger.getLogger(VertexForkJoin.class);
		log.info("Obtained sub result on Get Map Reduce");
		synchronized (this) {
			res.addAll(subres);
		}
		log.info("Added to final result.");
		return true;
	}

	@Override
	public TLongHashSet red(ArrayList<long[]> subres) throws Exception {
		Logger log = Logger.getLogger(VertexForkJoin.class);
		log.info("Finished obtaining results for Get MR");
		if (res == null)
			res = new TLongHashSet();
		return res;
	}
}