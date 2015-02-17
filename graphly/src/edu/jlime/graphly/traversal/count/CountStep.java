package edu.jlime.graphly.traversal.count;

import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;

public class CountStep implements Step {

	private Dir dir;
	private GraphlyTraversal tr;

	public CountStep(Dir dir, GraphlyTraversal gt) {
		this.dir = dir;
		this.tr = gt;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		Map<ClientNode, TLongArrayList> mapped = map.map(before.vertices()
				.toArray(), ctx);

		ForkJoinTask<TLongIntHashMap> fj = new ForkJoinTask<>();

		for (Entry<ClientNode, TLongArrayList> e : mapped.entrySet()) {
			fj.putJob(new CountJob(dir, e.getValue().toArray()), e.getKey());
		}

		TLongIntHashMap finalRes = fj
				.execute(new ResultListener<TLongIntHashMap, TLongIntHashMap>() {
					TLongIntHashMap ret = new TLongIntHashMap();

					@Override
					public void onSuccess(TLongIntHashMap subres) {
						TLongIntIterator it = subres.iterator();
						while (it.hasNext()) {
							it.advance();
							ret.adjustOrPutValue(it.key(), it.value(),
									it.value());
						}
					}

					@Override
					public TLongIntHashMap onFinished() {
						return ret;
					}

					@Override
					public void onFailure(Exception res) {
					}
				});
		return new CountResult(finalRes);
	}
}
