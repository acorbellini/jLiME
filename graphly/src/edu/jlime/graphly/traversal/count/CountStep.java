package edu.jlime.graphly.traversal.count;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
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

		List<Pair<ClientNode, TLongArrayList>> mapped = map.map(Graphly.MAX_IDS_PER_JOB, before
				.vertices().toArray(), ctx);

		ForkJoinTask<TLongIntHashMap> fj = new ForkJoinTask<>();

		for (Pair<ClientNode, TLongArrayList> e : mapped) {
			fj.putJob(new CountJob(dir, e.getValue().toArray()), e.getKey());
		}

		TLongFloatHashMap finalRes = fj
				.execute(new ResultListener<TLongIntHashMap, TLongFloatHashMap>() {
					TLongFloatHashMap ret = new TLongFloatHashMap();

					@Override
					public void onSuccess(TLongIntHashMap subres) {
						synchronized (ret) {
							TLongIntIterator it = subres.iterator();
							while (it.hasNext()) {
								it.advance();
								ret.adjustOrPutValue(it.key(), it.value(),
										it.value());
							}
						}
					}

					@Override
					public TLongFloatHashMap onFinished() {
						return ret;
					}

					@Override
					public void onFailure(Exception res) {
					}
				});
		return new CountResult(finalRes);
	}
}
