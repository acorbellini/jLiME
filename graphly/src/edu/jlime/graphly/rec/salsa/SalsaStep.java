package edu.jlime.graphly.rec.salsa;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaStep implements CustomFunction {
	private int steps;

	public SalsaStep(int steps) {
		this.steps = steps;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr)
			throws Exception {
		final Logger log = Logger.getLogger(SalsaStep.class);

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv()
				.getClientEnv(jobClient.getLocalPeer());

		TLongHashSet beforeSet = before.vertices();

		log.info("Executing Salsa Step on " + beforeSet.size());

		Graph g = tr.getGraph();

		Mapper map = (Mapper) tr.get("mapper");

		log.info("Filtering authority side");
		TLongHashSet authSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.IN, 1, beforeSet)).exec()
				.vertices();
		log.info("Filtering hub side");
		TLongHashSet hubSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.OUT, 1, beforeSet)).exec()
				.vertices();

		log.info("Executing SalsaRepeat with hubset " + hubSet.size()
				+ " and auth " + authSet.size());

		final TLongFloatHashMap auth = new TLongFloatHashMap();
		final TLongFloatHashMap hub = new TLongFloatHashMap();

		TLongHashSet subgraph = new TLongHashSet(authSet);
		subgraph.addAll(hubSet);

		TLongIterator it = subgraph.iterator();
		while (it.hasNext()) {
			long v = it.next();
			if (authSet.contains(v))
				auth.put(v, 1f / authSet.size());

			if (hubSet.contains(v))
				hub.put(v, 1f / hubSet.size());
		}

		for (int i = 0; i < steps; i++) {

			log.info("Executing Step " + i);

			final List<Pair<Node, TLongArrayList>> mapped = map
					.map(Graphly.NUM_JOBS, subgraph.toArray(), ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<Node, TLongArrayList> e : mapped)
				fj.putJob(new SalsaJob(tr.getGraph(), auth, hub, i, subgraph,
						e.getValue()), e.getKey());
			auth.clear();
			hub.clear();
			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {

				@Override
				public void onSuccess(AuthHubSubResult subres) {
					log.info("Received subresult.");
					synchronized (auth) {
						long[] auth_sub = subres.auth;
						float[] auth_sub_vals = subres.auth_vals;
						for (int i = 0; i < auth_sub.length; i++) {
							long key = auth_sub[i];
							float value = auth_sub_vals[i];
							auth.adjustOrPutValue(key, value, value);
						}
					}
					synchronized (hub) {
						long[] hub_sub = subres.hub;
						float[] hub_sub_vals = subres.hub_vals;
						for (int i = 0; i < hub_sub.length; i++) {
							long key = hub_sub[i];
							float value = hub_sub_vals[i];
							hub.adjustOrPutValue(key, value, value);
						}
					}
				}

				@Override
				public Void onFinished() {
					return null;
				}

				@Override
				public void onFailure(Exception res) {
				}
			});
		}

		return new AuthHubResult(auth, hub);
	}
}