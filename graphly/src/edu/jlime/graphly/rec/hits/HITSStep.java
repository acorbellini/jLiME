package edu.jlime.graphly.rec.hits;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.util.concurrent.AtomicDouble;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSStep implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;

	public HITSStep(String auth, String hub, int steps) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		Logger log = Logger.getLogger(HITSStep.class);

		long[] subgraph = before.vertices().toArray();

		GraphlyGraph g = tr.getGraph();

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		log.info("Executing HITS on " + subgraph.length + " vertices.");

		final TLongFloatHashMap auth = new TLongFloatHashMap();
		final TLongFloatHashMap hub = new TLongFloatHashMap();

		for (long v : subgraph) {
			auth.put(v, 1f / subgraph.length);
			hub.put(v, 1f / subgraph.length);
		}

		for (int i = 0; i < steps; i++) {

			System.out.println("Executing Step " + i);

			final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
					GraphlyClient.NUM_JOBS, subgraph, ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<ClientNode, TLongArrayList> e : mapped)
				fj.putJob(
						new HITSJob(tr.getGraph(), auth, hub, subgraph, e
								.getValue()), e.getKey());

			fj.execute(16, new ResultListener<AuthHubSubResult, Void>() {
				AtomicDouble sumAuth = new AtomicDouble(0d);
				AtomicDouble sumHub = new AtomicDouble(0d);

				@Override
				public void onSuccess(AuthHubSubResult subres) {
					System.out.println("Received subresult.");
					synchronized (auth) {
						auth.putAll(subres.auth);
					}
					synchronized (hub) {
						hub.putAll(subres.hub);
					}
					{
						TLongFloatIterator it = subres.auth.iterator();
						while (it.hasNext()) {
							it.advance();
							sumAuth.addAndGet(it.value());
						}
					}
					{
						TLongFloatIterator it = subres.hub.iterator();
						while (it.hasNext()) {
							it.advance();
							sumHub.addAndGet(it.value());
						}
					}
				}

				@Override
				public Void onFinished() {
					{
						TLongFloatIterator it = auth.iterator();
						while (it.hasNext()) {
							it.advance();
							it.setValue((float) (it.value() / sumAuth.get()));
						}
					}
					{
						TLongFloatIterator it = hub.iterator();
						while (it.hasNext()) {
							it.advance();
							it.setValue((float) (it.value() / sumHub.get()));
						}
					}
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
		g.setFloat(this.auth, auth);
		g.setFloat(this.hub, hub);
		// }

		return before;
	}

	@Override
	public String toString() {
		return "HITSCustomStep [auth=" + auth + ", hub=" + hub + ", steps="
				+ steps + "]";
	}
}