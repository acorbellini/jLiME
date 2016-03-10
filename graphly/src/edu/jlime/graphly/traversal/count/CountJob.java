package edu.jlime.graphly.traversal.count;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.client.ConsistentHashing;
import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class CountJob implements Job<Count> {

	private Dir dir;
	int max_edges;
	private Graph g;
	private long[] f;
	private long[] keys;
	private float[] values;

	public CountJob(Graph g, Dir dir, int max_edges, long[] keys, float[] values, long[] toFilter) {
		this.dir = dir;
		this.keys = keys;
		this.values = values;
		this.max_edges = max_edges;
		this.g = g;
		this.f = toFilter;
	}

	@Override
	public Count call(JobContext ctx, Node peer) throws Exception {
		final Logger log = Logger.getLogger(CountJob.class);

		log.info("Counting edges in dir " + dir + " with max " + max_edges + " and vertices " + keys.length + ".");
		TLongArrayList remote = getRemoteVertices();

		log.info("Preloading " + remote.size() + " vertices.");
		if (!remote.isEmpty())
			g.preload(remote);
		log.info("Finished preloading " + remote.size() + " vertices.");

		final TLongHashSet toFilter = new TLongHashSet(f == null ? new long[] {} : f);

		long init = System.currentTimeMillis();

		final int cores = Runtime.getRuntime().availableProcessors();

		ExecutorService exec = Executors.newFixedThreadPool(cores);

		int size = keys.length;
		final float chunks = (size / (float) cores);

		List<Future<TLongFloatMap>> results = new ArrayList<>();

		for (int i = 0; i < cores; i++) {
			final int tID = i;

			Future<TLongFloatMap> fut = exec.submit(new Callable<TLongFloatMap>() {
				@Override
				public TLongFloatMap call() throws Exception {
					int from = (int) (chunks * tID);
					int to = (int) (chunks * (tID + 1));

					if (tID == cores - 1)
						to = keys.length;
					long initChunk = System.currentTimeMillis();
					TLongFloatMap map = new TLongFloatHashMap();
					int cont = from;

					while (cont < to) {
						final long l = keys[cont];
						final float mult = values[cont];
						cont++;
						long[] curr = g.getEdgesMax(dir, max_edges, new long[] { l });
						for (long m : curr)
							if (toFilter == null || !toFilter.contains(m))
								map.adjustOrPutValue(m, mult, mult);

					}
					log.info("Finished chunk of size " + (to - from) + " in "
							+ (System.currentTimeMillis() - initChunk));

					return map;
				}
			});
			results.add(fut);
		}

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		final TLongFloatMap finalResult = new TLongFloatHashMap();
		for (Future<TLongFloatMap> future : results) {
			TLongFloatIterator itMap = future.get().iterator();
			while (itMap.hasNext()) {
				itMap.advance();
				finalResult.adjustOrPutValue(itMap.key(), itMap.value(), itMap.value());
			}
		}

		Count c = new Count(finalResult.keys(), finalResult.values());
		log.info("Finished count of " + keys.length + " (different) vertices resulting in " + finalResult.size()
				+ " vertices with counts in " + (System.currentTimeMillis() - init) + " ms");
		return c;
	}

	private TLongArrayList getRemoteVertices() {
		TLongArrayList remote = new TLongArrayList();
		ConsistentHashing hash = g.getHash();
		Peer localPeer = g.getJobClient().getLocalPeer();
		for (long v : keys)
			if (!hash.getNode(v).equals(localPeer))
				remote.add(v);
		return remote;
	}
}
