package edu.jlime.graphly.rec;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.rec.salsa.SalsaStep;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
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
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		final Logger log = Logger.getLogger(SalsaStep.class);

		TLongHashSet beforeSet = before.vertices();
		log.info("Executing Salsa FJ Step on " + beforeSet.size());

		Graph g = tr.getGraph();

		log.info("Filtering authority side");
		TLongHashSet authSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.IN, 1, beforeSet)).exec().vertices();
		log.info("Filtering hub side");
		TLongHashSet hubSet = g.v(beforeSet).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.OUT, 1, beforeSet)).exec().vertices();

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		log.info("Executing SalsaRepeat with hubset " + hubSet.size() + " and auth " + authSet.size());

		TLongHashSet subgraph = new TLongHashSet(authSet);
		subgraph.addAll(hubSet);

		int authSize = authSet.size();
		int hubSize = hubSet.size();

		for (int i = 0; i < steps; i++) {

			log.info("Executing Step " + i);

			final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, subgraph.toArray(), ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<Node, TLongArrayList> e : mapped)
				fj.putJob(new SalsaHybridJob(tr.getGraph(), authSize, hubSize, auth, hub, i, subgraph, e.getValue()),
						e.getKey());

			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {

				@Override
				public void onSuccess(AuthHubSubResult subres) {
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
		TLongFloatHashMap authRes = new TLongFloatHashMap();
		TLongFloatHashMap hubRes = new TLongFloatHashMap();
		
		Set<Pair<Long, Float>> set = g.topFloat(auth, top);
		for (Pair<Long, Float> pair : set) {
			authRes.put(pair.left, pair.right);
		}
		Set<Pair<Long, Float>> setHub = g.topFloat(hub, top);
		for (Pair<Long, Float> pair : setHub) {
			hubRes.put(pair.left, pair.right);
		}
		return new AuthHubResult(authRes, hubRes);
	}

}
