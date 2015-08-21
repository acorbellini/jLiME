package edu.jlime.graphly.rec;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.count.CountJob;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphCount implements Job<TLongHashSet> {

	private GraphlyGraph g;
	private Dir dir;
	private int max;
	private long[] data;
	private String k;

	public GraphCount(GraphlyGraph graph, String k, Dir dir, int max, long[] ls) {
		this.g = graph;
		this.dir = dir;
		this.max = max;
		this.data = ls;
		this.k = k;
	}

	@Override
	public TLongHashSet call(JobContext env, ClientNode peer) throws Exception {
		final Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing graph count job for " + data.length);
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		final TLongHashSet finalRes = new TLongHashSet();
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						TLongFloatHashMap sub = new TLongFloatHashMap();
						int cont = 0;
						for (long v : data) {
							cont++;
							if (cont % threads == tID) {
								float prevCount = g.getFloat(v, k, 1f);
								long[] edges = g.getEdgesMax(dir, max, v);
								for (int j = 0; j < edges.length; j++) {
									sub.adjustOrPutValue(edges[j], prevCount,
											prevCount);
								}
							}
						}

						log.info("Finished counting vertices for thread " + tID);

						g.setTempFloats(k, true, sub);

						log.info("Finished sending results to graph for thread "
								+ tID);
						synchronized (finalRes) {
							finalRes.addAll(sub.keys());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		return finalRes;

	}
}
