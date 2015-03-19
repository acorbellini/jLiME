package edu.jlime.graphly.traversal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.graphly.rec.VertexFilter;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class CustomFilterStep implements Step {

	private VertexFilter f;
	private GraphlyTraversal tr;

	public CustomFilterStep(VertexFilter f, GraphlyTraversal tr) {
		this.f = f;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		ExecutorService exec = Executors.newFixedThreadPool(64);
		final TLongHashSet ret = new TLongHashSet();
		TLongIterator it = before.vertices().iterator();
		while (it.hasNext()) {
			final long next = it.next();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						if (f.filter(next, tr.getGraph())) {
							synchronized (ret) {
								ret.add(next);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		return new VertexResult(ret);
	}

}
