package edu.jlime.graphly.rec.hits;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.storenode.StoreNodeImpl;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.util.Gather;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSHybridStep implements CustomFunction {
	public static class NormalizeHitsValue implements Gather<Void> {
		private String k;
		private float sum;

		public NormalizeHitsValue(String k, float sum) {
			this.k = k;
			this.sum = sum;
		}

		@Override
		public Void gather(String graph, StoreNodeImpl node) throws Exception {
			TLongFloatIterator it = node.getFloatIterator(graph, k);
			while (it.hasNext()) {
				it.advance();
				it.setValue(it.value() / sum);
			}
			return null;
		}
	}

	private String auth;
	private String hub;
	private int steps;
	private int top;

	public HITSHybridStep(String auth, String hub, int steps, int top) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		final Logger log = Logger.getLogger(HITSStep.class);

		long[] subgraph = before.vertices().toArray();

		Graph g = tr.getGraph();

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		log.info("Executing HITS on " + subgraph.length + " vertices.");

		final TLongFloatHashMap authMap = new TLongFloatHashMap();
		final TLongFloatHashMap hubMap = new TLongFloatHashMap();

		for (long v : subgraph) {
			authMap.put(v, 1f / subgraph.length);
			hubMap.put(v, 1f / subgraph.length);
		}

		g.setFloat(auth, authMap);
		g.setFloat(hub, hubMap);

		for (int i = 0; i < steps; i++) {

			log.info("Executing Step " + i);

			final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, subgraph, ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<Node, TLongArrayList> e : mapped)
				fj.putJob(new HITSHybridJob(tr.getGraph(), auth, hub, subgraph, e.getValue()), e.getKey());

			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {

				@Override
				public void onSuccess(AuthHubSubResult subres) {
					log.info("Received subresult.");
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

			g.gather(new NormalizeHitsValue(auth, g.sumFloat(auth)));
			g.gather(new NormalizeHitsValue(hub, g.sumFloat(hub)));
		}

		// TLongIterator itResult = subgraph.iterator();
		// while (itResult.hasNext()) {
		// long v = itResult.next();
		// g.setFloat(this.auth, auth);
		// g.setFloat(this.hub, hub);
		// }
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

	@Override
	public String toString() {
		return "HITSCustomStep [auth=" + auth + ", hub=" + hub + ", steps=" + steps + "]";
	}
}
