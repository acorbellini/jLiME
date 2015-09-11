package edu.jlime.graphly.rec.hits;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSJob implements Job<AuthHubSubResult> {

	private Graph g;
	private TLongFloatHashMap auth;
	private TLongFloatHashMap hub;
	private long[] subgraph;
	private TLongArrayList vertices;

	public HITSJob(Graph graph, TLongFloatHashMap auth, TLongFloatHashMap hub, long[] subgraph,
			TLongArrayList value) {
		this.g = graph;
		this.auth = new TLongFloatHashMap(auth);
		this.hub = new TLongFloatHashMap(hub);
		this.subgraph = subgraph;
		this.vertices = value;
	}

	@Override
	public AuthHubSubResult call(JobContext env, Node peer) throws Exception {
		final SubGraph sg = g.getSubGraph("hitsg", this.subgraph);
		ArrayList<Future<AuthHubSubResult>> futs = new ArrayList<>();
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		TLongFloatHashMap authRes = new TLongFloatHashMap();
		TLongFloatHashMap hubRes = new TLongFloatHashMap();

		int size = vertices.size();

		final float chunks = (size / (float) threads);

		for (int i = 0; i < threads; i++) {
			final int tID = i;
			Future<AuthHubSubResult> fut = exec.submit(new Callable<AuthHubSubResult>() {
				@Override
				public AuthHubSubResult call() throws Exception {
					TLongFloatHashMap authRes = new TLongFloatHashMap();
					TLongFloatHashMap hubRes = new TLongFloatHashMap();

					int from = (int) (chunks * tID);
					int to = (int) (chunks * (tID + 1));

					if (tID == threads - 1)
						to = vertices.size();

					int cont = from;
					while (cont < to) {
						long v = vertices.get(cont++);
						long[] outgoing = sg.getEdges(Dir.OUT, v);
						float value = hub.get(v);
						if (value != hub.getNoEntryValue()) {
							for (long out : outgoing) {
								authRes.adjustOrPutValue(out, value, value);
							}
						}

						long[] incoming = sg.getEdges(Dir.IN, v);
						float value2 = auth.get(v);
						if (value2 != auth.getNoEntryValue()) {
							for (long in : incoming) {
								hubRes.adjustOrPutValue(in, value2, value2);
							}
						}
					}
					return new AuthHubSubResult(authRes, hubRes);
				}
			});
			futs.add(fut);
		}
		exec.shutdown();
		for (Future<AuthHubSubResult> future : futs) {
			AuthHubSubResult authHubSubResult = future.get();
			TLongFloatIterator it = authHubSubResult.auth.iterator();
			while (it.hasNext()) {
				it.advance();
				long key = it.key();
				float value = it.value();

				authRes.adjustOrPutValue(key, value, value);
			}

			TLongFloatIterator itHub = authHubSubResult.hub.iterator();
			while (itHub.hasNext()) {
				itHub.advance();
				hubRes.adjustOrPutValue(itHub.key(), itHub.value(), itHub.value());
			}
		}

		return new AuthHubSubResult(authRes, hubRes);
	}

}
