package edu.jlime.graphly.traversal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;

public class CountForkJoin extends
		MapReduceTask<long[], TLongIntHashMap, TLongIntHashMap> {

	private Dir dir;
	private Mapper mapper;
	private TLongIntHashMap ret = new TLongIntHashMap();

	public CountForkJoin(long[] data, Dir dir, Mapper mapper) {
		super(data);
		setDontCacheSubResults(true);
		this.dir = dir;
		this.mapper = mapper;
	}

	@Override
	public Map<Job<TLongIntHashMap>, ClientNode> map(long[] data, JobContext env)
			throws Exception {
		HashMap<Job<TLongIntHashMap>, ClientNode> res = new HashMap<>();

		Map<ClientNode, TLongArrayList> mapped = mapper.map(data, env);

		for (Entry<ClientNode, TLongArrayList> e : mapped.entrySet()) {
			res.put(new CountForkJoinJob(dir, e.getValue().toArray()),
					e.getKey());
		}
		return res;
	}

	@Override
	public boolean processSubResult(TLongIntHashMap subres) {
		insert(subres);
		return true;
	}

	private void insert(TLongIntHashMap subres) {
		synchronized (ret) {
			TLongIntIterator it = subres.iterator();
			while (it.hasNext()) {
				it.advance();
				ret.adjustOrPutValue(it.key(), it.value(), it.value());
			}
		}
	}

	@Override
	public TLongIntHashMap red(ArrayList<TLongIntHashMap> subres)
			throws Exception {
		for (TLongIntHashMap tLongIntHashMap : subres)
			insert(tLongIntHashMap);		
		return ret;
	}

}
