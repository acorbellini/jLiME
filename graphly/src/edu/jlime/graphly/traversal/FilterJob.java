package edu.jlime.graphly.traversal;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.VertexFilter;
import edu.jlime.graphly.rec.salsa.AuthHubSubResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class FilterJob implements Job<TLongHashSet> {

	private GraphlyGraph g;
	private VertexFilter f;
	private long[] array;

	public FilterJob(GraphlyGraph graph, VertexFilter f, long[] array) {
		this.g = graph;
		this.f = f;
		this.array = array;
	}

	@Override
	public TLongHashSet call(JobContext env, ClientNode peer) throws Exception {
		ArrayList<Future<TLongHashSet>> futs = new ArrayList<>();
		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		TLongHashSet ret = new TLongHashSet();
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			Future<TLongHashSet> fut = exec
					.submit(new Callable<TLongHashSet>() {
						@Override
						public TLongHashSet call() throws Exception {
							TLongHashSet ret = new TLongHashSet();
							int cont = 0;
							for (long v : array) {
								cont++;
								if (cont % threads == tID) {
									if (f.filter(v, g)) {
										ret.add(v);
									}
								}
							}
							return ret;
						}
					});
			futs.add(fut);
		}
		exec.shutdown();
		for (Future<TLongHashSet> future : futs) {
			ret.addAll(future.get());
		}
		return ret;
	}
}
