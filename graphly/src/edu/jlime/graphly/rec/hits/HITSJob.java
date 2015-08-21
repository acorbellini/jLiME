package edu.jlime.graphly.rec.hits;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSJob implements Job<AuthHubSubResult> {

	private GraphlyGraph g;
	private TLongFloatHashMap auth;
	private TLongFloatHashMap hub;
	private long[] subgraph;
	private TLongArrayList vertices;

	public HITSJob(GraphlyGraph graph, TLongFloatHashMap auth,
			TLongFloatHashMap hub, long[] subgraph, TLongArrayList value) {
		this.g = graph;
		this.auth = new TLongFloatHashMap(auth);
		this.hub = new TLongFloatHashMap(hub);
		this.subgraph = subgraph;
		this.vertices = value;
	}

	@Override
	public AuthHubSubResult call(JobContext env, ClientNode peer)
			throws Exception {
		final SubGraph sg = g.getSubGraph("hitsg", this.subgraph);
		ArrayList<Future<AuthHubSubResult>> futs = new ArrayList<>();
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		TLongFloatHashMap authRes = new TLongFloatHashMap();
		TLongFloatHashMap hubRes = new TLongFloatHashMap();
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			Future<AuthHubSubResult> fut = exec
					.submit(new Callable<AuthHubSubResult>() {
						@Override
						public AuthHubSubResult call() throws Exception {
							TLongFloatHashMap authRes = new TLongFloatHashMap();
							TLongFloatHashMap hubRes = new TLongFloatHashMap();
							TLongIterator it = vertices.iterator();
							int cont = 0;
							while (it.hasNext()) {
								cont++;
								if (cont % threads == tID) {
									long v = it.next();
									float sumAuth = 0f;
									long[] incomingEdges = sg.getEdges(Dir.IN,
											v);
									for (long in : incomingEdges)
										sumAuth += hub.get(in);
									authRes.put(v, sumAuth);

									float sumHub = 0f;
									long[] outEdges = sg.getEdges(Dir.OUT, v);
									for (long out : outEdges)
										sumHub += auth.get(out);
									hubRes.put(v, sumHub);
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
			authRes.putAll(authHubSubResult.auth);
			hubRes.putAll(authHubSubResult.hub);
		}
		return new AuthHubSubResult(authRes, hubRes);
	}

}
