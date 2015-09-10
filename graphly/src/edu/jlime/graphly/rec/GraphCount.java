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
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphCount implements Job<long[]> {

	private static final int MAX = 10000;
	private GraphlyGraph g;
	private Dir dir;
	private int max;
	private long[] data;
	private String k;
	private TLongHashSet filters;
	private boolean returnVertices;

	public GraphCount(TLongHashSet filters2, GraphlyGraph graph, String k,
			Dir dir, int max, long[] ls, boolean returnVertices) {
		this.filters = filters2;
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

		final TLongFloatHashMap finalRes = new TLongFloatHashMap(10000000);
		// final TLongHashSet finalRes = new TLongHashSet();
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						int from = (int) (chunks * tID);
						int to = (int) (chunks * (tID + 1));

						if (tID == threads - 1)
							to = keys.length;

						TLongFloatHashMap sub = new TLongFloatHashMap(100000);
						int cont = from;
						while (cont < to) {
							long v = keys[cont++];
							float prevCount = g.getFloat(v, k, 1f);
							long[] edges = g.getEdgesMax(dir, max, v);
							if (edges.length > 0)

								for (int j = 0; j < edges.length; j++) {
									long key = edges[j];
									if (filters == null
											|| !filters.contains(key)) {
										sub.adjustOrPutValue(key, prevCount,
												prevCount);
									}
								}

						}

						synchronized (finalRes) {
							TLongFloatIterator itMap = sub.iterator();
							while (itMap.hasNext()) {
								itMap.advance();
								finalRes.adjustOrPutValue(itMap.key(),
										itMap.value(), itMap.value());
							}
						}

						// if (returnVertices)
						// synchronized (finalRes) {
						// finalRes.addAll(sub.keys());
						// }

						// if (!sub.isEmpty()) {
						// log.info("Setting temp floats of size " + sub.size()
						// + " on thread " + tID);
						// g.setTempFloats(k, true, sub);
						// }
						log.info(
								"Finished counting vertices for thread " + tID);
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		if (!finalRes.isEmpty()) {
			log.info("Sending results " + finalRes.size());
			g.setTempFloats(k, true, finalRes);
		}
		if (!returnVertices)
			return new long[] {};
		return finalRes.keys();

	}
}
