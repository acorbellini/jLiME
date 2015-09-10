package edu.jlime.graphly.rec;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.rec.salsa.SalsaJob;
import edu.jlime.graphly.rec.salsa.SalsaStep;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaHybrid implements CustomFunction {

	private int steps;
	private String auth;
	private String hub;
	private int top;

	public SalsaHybrid(String auth, String hub, int steps, int top) {
		this.steps = steps;
		this.auth = auth;
		this.hub = hub;
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr) throws Exception {
		final Logger log = Logger.getLogger(SalsaStep.class);

		TLongHashSet beforeSet = before.vertices();
		log.info("Executing Salsa FJ Step on " + beforeSet.size());

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

		TLongHashSet subgraph = new TLongHashSet(authSet);
		subgraph.addAll(hubSet);
		//
		// TLongIterator it = subgraph.iterator();
		// while (it.hasNext()) {
		// long v = it.next();
		// if (authSet.contains(v))
		// auth.put(v, 1f / authSet.size());
		//
		// if (hubSet.contains(v))
		// hub.put(v, 1f / hubSet.size());
		// }

		int authSize = authSet.size();
		int hubSize = hubSet.size();

		for (int i = 0; i < steps; i++) {

			log.info("Executing Step " + i);

			final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(GraphlyClient.NUM_JOBS, subgraph.toArray(),
					ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<ClientNode, TLongArrayList> e : mapped)
				fj.putJob(new SalsaHybridJob(tr.getGraph(), authSize, hubSize, auth, hub, i, subgraph, e.getValue()),
						e.getKey());

			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {

				@Override
				public void onSuccess(AuthHubSubResult subres) {
					// log.info("Received subresult.");
					// synchronized (auth) {
					// auth.putAll(subres.auth);
					// }
					// synchronized (hub) {
					// hub.putAll(subres.hub);
					// }
				}

				@Override
				public Void onFinished() {
					return null;
				}

				@Override
				public void onFailure(Exception res) {
				}
			});

			g.commitFloatUpdates(this.auth);
			g.commitFloatUpdates(this.hub);

		}

		log.info("Counting top " + top);
		Set<Pair<Long, Float>> set = g.topFloat(auth, top);

		TLongFloatHashMap authRes = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			authRes.put(pair.left, pair.right);
		}

		Set<Pair<Long, Float>> setHub = g.topFloat(hub, top);

		TLongFloatHashMap hubRes = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : setHub) {
			hubRes.put(pair.left, pair.right);
		}
		return new AuthHubResult(authRes, hubRes);
	}

}
