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

public class GraphCount implements Job<long[]> {

	private static final int MAX = 10000;
	private GraphlyGraph g;
	private Dir dir;
	private int max;
	private long[] data;
	private String k;
	private String[] filters;
	private boolean returnVertices;

	public GraphCount(String[] filters, GraphlyGraph graph, String k, Dir dir,
			int max, long[] ls, boolean returnVertices) {
		this.filters = filters;
		this.g = graph;
		this.dir = dir;
		this.max = max;
		this.data = ls;
		this.k = k;
		this.returnVertices = returnVertices;
	}

	@Override
	public long[] call(JobContext env, ClientNode peer) throws Exception {
		final Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing graph count job for " + data.length);
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);

		final long[] keys = data;
		int size = keys.length;
		final float chunks = (size / (float) threads);

		final TLongHashSet finalRes = new TLongHashSet(100000);
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						int from = (int) (chunks * tID);
						int to = (int) (chunks * (tID + 1));

						// GraphCache cache = new GraphCache(data, from, to, g,
						// MAX);

						if (tID == threads - 1)
							to = keys.length;

						TLongFloatHashMap sub = new TLongFloatHashMap(100000);
						int cont = from;

						while (cont < to) {
							long v = keys[cont++];
							boolean filtered = false;
							if (filters != null && filters.length > 0) {
								Object mark = g.getProperty("mark", v);
								if (mark != null)
									for (String f : filters) {
										if (f.equals(mark))
											filtered = true;
									}
							}

							if (!filtered) {
								float prevCount = g.getFloat(v, k, 1f);
								long[] edges = g.getEdgesMax(dir, max, v);
								for (int j = 0; j < edges.length; j++) {
									sub.adjustOrPutValue(edges[j], prevCount,
											prevCount);
								}
							}

						}

						log.info("Finished counting vertices for thread " + tID);
						if (!sub.isEmpty())
							g.setTempFloats(k, true, sub);

						log.info("Finished sending results to graph for thread "
								+ tID);
						if (returnVertices) {
							log.info("merging vertices processed");
							synchronized (finalRes) {
								finalRes.addAll(sub.keySet());
							}
							log.info("Finished merging results on thread "
									+ tID);
						}

					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		return finalRes.toArray();

	}
}
