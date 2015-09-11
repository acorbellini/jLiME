package edu.jlime.graphly.rec.hits;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSStep implements CustomFunction {
	private int steps;

	public HITSStep(int steps) {
		this.steps = steps;
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

		final TLongFloatHashMap auth = new TLongFloatHashMap();
		final TLongFloatHashMap hub = new TLongFloatHashMap();

		for (long v : subgraph) {
			auth.put(v, 1f / subgraph.length);
			hub.put(v, 1f / subgraph.length);
		}

		for (int i = 0; i < steps; i++) {

			log.info("Executing Step " + i);

			final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, subgraph, ctx);

			ForkJoinTask<AuthHubSubResult> fj = new ForkJoinTask<>();

			for (Pair<Node, TLongArrayList> e : mapped)
				fj.putJob(new HITSJob(tr.getGraph(), auth, hub, subgraph, e.getValue()), e.getKey());

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
					float sumAuth = 0f;
					float sumHub = 0f;
					{
						TLongFloatIterator it = auth.iterator();
						while (it.hasNext()) {
							it.advance();
							sumAuth += it.value();
						}
					}
					{
						TLongFloatIterator it = hub.iterator();
						while (it.hasNext()) {
							it.advance();
							sumHub += it.value();
						}
					}
					{
						TLongFloatIterator it = auth.iterator();
						while (it.hasNext()) {
							it.advance();
							it.setValue((float) (it.value() / sumAuth));
						}
					}
					{
						TLongFloatIterator it = hub.iterator();
						while (it.hasNext()) {
							it.advance();
							it.setValue((float) (it.value() / sumHub));
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
		// g.setFloat(this.auth, auth);
		// g.setFloat(this.hub, hub);
		// }
		float sumAuth = 0f;
		float sumHub = 0f;
		TLongFloatIterator itAuth = auth.iterator();
		while (itAuth.hasNext()) {
			itAuth.advance();
			sumAuth += itAuth.value();
		}

		TLongFloatIterator itHub = hub.iterator();
		while (itHub.hasNext()) {
			itHub.advance();
			sumHub += itHub.value();
		}

		System.out.println(sumAuth);
		System.out.println(sumHub);

		return new AuthHubResult(auth, hub);
	}

	@Override
	public String toString() {
		return "HITSCustomStep [ steps=" + steps + "]";
	}
}