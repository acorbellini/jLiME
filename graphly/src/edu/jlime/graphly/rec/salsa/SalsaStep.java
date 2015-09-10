package edu.jlime.graphly.rec.salsa;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
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
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr) throws Exception {
		final Logger log = Logger.getLogger(SalsaStep.class);

		TLongHashSet beforeSet = before.vertices();
		log.info("Executing Salsa Step on " + beforeSet.size());

		GraphlyGraph g = tr.getGraph();

		log.info("Filtering authority side");
		TLongHashSet authSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.IN, 1, beforeSet)).exec().vertices();
		log.info("Filtering hub side");
		TLongHashSet hubSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.OUT, 1, beforeSet)).exec().vertices();

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		log.info("Executing SalsaRepeat with hubset " + hubSet.size() + " and auth " + authSet.size());

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

			final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(GraphlyClient.NUM_JOBS, subgraph.toArray(),
					ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<ClientNode, TLongArrayList> e : mapped)
				fj.putJob(new SalsaJob(tr.getGraph(), auth, hub, i, subgraph, e.getValue()), e.getKey());
			auth.clear();
			hub.clear();
			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {

				@Override
				public void onSuccess(AuthHubSubResult subres) {
					log.info("Received subresult.");
					synchronized (auth) {
						TLongFloatIterator itAuth = subres.auth.iterator();
						while (itAuth.hasNext()) {
							itAuth.advance();
							auth.adjustOrPutValue(itAuth.key(), itAuth.value(), itAuth.value());
						}
					}
					synchronized (hub) {
						TLongFloatIterator itHub = subres.hub.iterator();
						while (itHub.hasNext()) {
							itHub.advance();
							hub.adjustOrPutValue(itHub.key(), itHub.value(), itHub.value());
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

		// TLongIterator itResult = subgraph.iterator();
		// while (itResult.hasNext()) {
		// long v = itResult.next();
		// g.setFloat(this.auth, auth);
		// g.setFloat(this.hub, hub);
		// }

		return new AuthHubResult(auth, hub);
	}
}