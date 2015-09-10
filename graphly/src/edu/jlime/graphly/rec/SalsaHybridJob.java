package edu.jlime.graphly.rec;

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
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaHybridJob implements Job<AuthHubSubResult> {

	private GraphlyGraph g;
	private int step;
	private long[] subgraph;
	private TLongArrayList vertices;
	private String hub;
	private String auth;
	private int authSize;
	private int hubSize;

	public SalsaHybridJob(GraphlyGraph graph, int authSize, int hubSize,
			String auth, String hub, int i, TLongHashSet subgraph,
			TLongArrayList value) {
		this.authSize = authSize;
		this.hubSize = hubSize;
		this.g = graph;
		this.step = i;
		this.subgraph = subgraph.toArray();
		this.vertices = value;
		this.auth = auth;
		this.hub = hub;
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

		int size = vertices.size();
		final float chunks = (size / (float) threads);

		for (int i = 0; i < threads; i++) {
			final int tID = i;
			Future<AuthHubSubResult> fut = exec
					.submit(new Callable<AuthHubSubResult>() {
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

								if (step % 2 == 0) {
									// AUTH
									long[] inW = sg.getEdges(Dir.IN, v);
									if (inW.length > 0) {
										float a = g.getFloat(v, auth,
												1f / authSize);
										float value = a / inW.length;
										for (long w : inW)
											authRes.adjustOrPutValue(w, value,
													value);
									}

									// HUB
									long[] outW = sg.getEdges(Dir.OUT, v);
									if (outW.length > 0) {
										float h = g.getFloat(v, hub,
												1f / hubSize);
										float f = h / outW.length;
										for (long w : outW)
											hubRes.adjustOrPutValue(w, f, f);
									}

								} else if (step % 2 == 1) {
									long[] outV = sg.getEdges(Dir.OUT, v);
									float authsum = g.getFloat(v, auth);
									float value = authsum / outV.length;
									for (long w : outV)
										authRes.adjustOrPutValue(w, value,
												value);

									long[] inV = sg.getEdges(Dir.IN, v);
									float hubSum = g.getFloat(v, hub);
									float hubvalue = hubSum / inV.length;
									for (long w : inV)
										hubRes.adjustOrPutValue(w, hubvalue,
												hubvalue);
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
				authRes.adjustOrPutValue(it.key(), it.value(), it.value());
			}

			TLongFloatIterator itHub = authHubSubResult.hub.iterator();
			while (itHub.hasNext()) {
				itHub.advance();
				hubRes.adjustOrPutValue(itHub.key(), itHub.value(),
						itHub.value());
			}
		}

		g.setTempFloats(auth, true, authRes);
		g.setTempFloats(hub, true, hubRes);

		// return new AuthHubSubResult(authRes, hubRes);
		return null;
	}

}
