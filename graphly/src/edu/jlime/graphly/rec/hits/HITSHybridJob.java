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
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSHybridJob implements Job<AuthHubSubResult> {

	private Graph g;
	private String auth;
	private String hub;
	private long[] subgraph;
	private TLongArrayList vertices;

	public HITSHybridJob(Graph graph, String auth, String hub, long[] subgraph,
			TLongArrayList value) {
		this.g = graph;
		this.auth = auth;
		this.hub = hub;
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
								long[] outgoing = sg.getEdges(Dir.OUT, v);
								float value = g.getFloat(v, hub, 0f);
								for (long out : outgoing)
									authRes.adjustOrPutValue(out, value, value);

								long[] incoming = sg.getEdges(Dir.IN, v);
								float value2 = g.getFloat(v, auth, 0f);
								for (long in : incoming)
									hubRes.adjustOrPutValue(in, value2, value2);
							}
							return new AuthHubSubResult(authRes.keys(),
									authRes.values(), hubRes.keys(),
									hubRes.values());
						}
					});
			futs.add(fut);
		}
		exec.shutdown();
		for (Future<AuthHubSubResult> future : futs) {
			AuthHubSubResult authHubSubResult = future.get();
			{
				long[] auth_sub = authHubSubResult.auth;
				float[] auth_sub_vals = authHubSubResult.auth_vals;
				for (int i = 0; i < auth_sub.length; i++) {
					long key = auth_sub[i];
					float value = auth_sub_vals[i];
					authRes.adjustOrPutValue(key, value, value);
				}
			}
			{
				long[] hub_sub = authHubSubResult.hub;
				float[] hub_sub_vals = authHubSubResult.hub_vals;
				for (int i = 0; i < hub_sub.length; i++) {
					long key = hub_sub[i];
					float value = hub_sub_vals[i];
					hubRes.adjustOrPutValue(key, value, value);
				}
			}
		}

		g.setTempFloats(auth, true, authRes);
		g.setTempFloats(hub, true, hubRes);

		return null;
	}

}
