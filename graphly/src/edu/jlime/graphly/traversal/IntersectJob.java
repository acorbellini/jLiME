package edu.jlime.graphly.traversal;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.client.Graph;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class IntersectJob implements Job<TLongHashSet> {

	private Graph g;
	private Dir dir;
	private TLongArrayList list;

	public IntersectJob(Graph graph, Dir dir, TLongArrayList tLongArrayList) {
		this.g = graph;
		this.dir = dir;
		this.list = tLongArrayList;
	}

	@Override
	public TLongHashSet call(JobContext env, Node peer) throws Exception {
		final int threads = Runtime.getRuntime().availableProcessors();
		int size = list.size();
		final float chunks = (size / (float) threads);

		ExecutorService exec = Executors.newCachedThreadPool();
		ArrayList<Future<TLongHashSet>> futures = new ArrayList<>();
		for (int i = 0; i < threads; i++) {
			final int tID = i;
			futures.add(exec.submit(new Callable<TLongHashSet>() {
				@Override
				public TLongHashSet call() throws Exception {
					int from = (int) (chunks * tID);
					int to = (int) (chunks * (tID + 1));

					TLongHashSet ret = null;
					for (int j = from; j < to; j++) {
						long v = list.get(j);
						TLongHashSet vres = new TLongHashSet(
								g.getEdges(dir, v));
						if (ret == null)
							ret = vres;
						else {
							TLongIterator retIt = ret.iterator();
							while (retIt.hasNext()) {
								if (!vres.contains(retIt.next()))
									retIt.remove();
							}
						}
					}
					return ret;
				}
			}));
		}

		exec.shutdown();
		{
			TLongHashSet ret = null;
			for (Future<TLongHashSet> future : futures) {
				TLongHashSet vres = future.get();
				if (ret == null)
					ret = vres;
				else {
					TLongIterator retIt = ret.iterator();
					while (retIt.hasNext()) {
						if (!vres.contains(retIt.next()))
							retIt.remove();
					}
				}
			}
			return ret;
		}
	}
}
