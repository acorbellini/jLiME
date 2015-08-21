package edu.jlime.graphly.rec.salsa;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaJob implements Job<AuthHubSubResult> {

	private GraphlyGraph g;
	private TLongFloatHashMap auth;
	private TLongFloatHashMap hub;
	private int step;
	private long[] subgraph;
	private TLongArrayList vertices;

	public SalsaJob(GraphlyGraph graph, TLongFloatHashMap auth,
			TLongFloatHashMap hub, int i, TLongHashSet subgraph,
			TLongArrayList value) {
		this.g = graph;
		this.auth = new TLongFloatHashMap(auth);
		this.hub = new TLongFloatHashMap(hub);
		this.step = i;
		this.subgraph = subgraph.toArray();
		this.vertices = value;
	}

	@Override
	public AuthHubSubResult call(JobContext env, ClientNode peer)
			throws Exception {
		ArrayList<Future<AuthHubSubResult>> futs = new ArrayList<>();
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		final SubGraph sg = g.getSubGraph("salsasg", this.subgraph);
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
								long v = it.next();
								cont++;
								if (cont % threads == tID) {
									if (step % 3 == 0) {
										int inW = sg.getEdgesCount(Dir.IN, v);
										if (inW > 0)
											authRes.put(v, auth.get(v) / inW);
										else
											authRes.put(v, 0f);

										int outW = sg.getEdgesCount(Dir.OUT, v);
										if (outW > 0)
											hubRes.put(v, hub.get(v) / outW);
										else
											hubRes.put(v, 0f);

									} else if (step % 3 == 1) {
										long[] outV = sg.getEdges(Dir.OUT, v);
										float authsum = 0f;
										for (long w : outV)
											authsum += auth.get(w);

										if (outV.length > 0)
											authRes.put(v, authsum
													/ outV.length);
										else
											authRes.put(v, 0f);

										long[] inV = sg.getEdges(Dir.IN, v);
										float hubsum = 0f;
										for (long w : inV)
											hubsum += hub.get(w);

										if (inV.length > 0)
											hubRes.put(v, hubsum / inV.length);
										else
											hubRes.put(v, 0f);
									} else {
										long[] inV = sg.getEdges(Dir.IN, v);
										float authsum = 0f;
										for (long w : inV)
											authsum += auth.get(w);
										authRes.put(v, authsum);

										long[] outV = sg.getEdges(Dir.OUT, v);
										float hubsum = 0f;
										for (long w : outV)
											hubsum += hub.get(w);
										hubRes.put(v, hubsum);
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
			authRes.putAll(authHubSubResult.auth);
			hubRes.putAll(authHubSubResult.hub);
		}
		return new AuthHubSubResult(authRes, hubRes);
	}
}
