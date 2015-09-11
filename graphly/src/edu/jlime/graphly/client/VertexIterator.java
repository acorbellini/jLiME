package edu.jlime.graphly.client;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import edu.jlime.graphly.storenode.rpc.StoreNode;
import gnu.trove.list.array.TLongArrayList;

public class VertexIterator implements Iterator<Long> {
	ExecutorService exec = Executors.newCachedThreadPool(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = Executors.defaultThreadFactory().newThread(r);
			t.setName("Vertex Iterator Thread");
			t.setDaemon(true);
			return t;
		}
	});

	// private Graphly g;

	private StoreNode[] nodes;

	Future<TLongArrayList>[] fut = null;

	TLongArrayList cached = null;

	int current = 0;

	int currentNodeIndex = 0;

	private int max;

	private String gID;

	public VertexIterator(String graph, List<StoreNode> nodes, int cached) {
		// this.g = graphly;
		this.gID = graph;
		this.nodes = new StoreNode[nodes.size()];
		int nodeIndex = 0;
		for (StoreNode graphlyStoreNodeI : nodes) {
			this.nodes[nodeIndex++] = graphlyStoreNodeI;
		}

		this.max = cached;

		fut = new Future[nodes.size()];
		for (int i = 0; i < fut.length; i++) {
			final StoreNode node = this.nodes[i];
			fut[i] = exec.submit(new Callable<TLongArrayList>() {
				@Override
				public TLongArrayList call() throws Exception {
					return node.getVertices(gID, Long.MIN_VALUE, max, true);
				}
			});
		}
	}

	public boolean hasNext() {
		try {
			if (cached != null && current < cached.size())
				return true;
			else
				cached = null;

			this.current = 0;
			int loopcheck = 0;
			while (cached == null && loopcheck < nodes.length) {
				if (fut[currentNodeIndex] != null) {
					cached = fut[currentNodeIndex].get();
					if (cached == null || cached.isEmpty()) {
						cached = null;
						fut[currentNodeIndex] = null;
					} else {
						final long currentMin = cached.get(cached.size() - 1);
						final StoreNode node = nodes[currentNodeIndex];
						fut[currentNodeIndex] = exec.submit(new Callable<TLongArrayList>() {
							@Override
							public TLongArrayList call() throws Exception {
								return node.getVertices(gID, currentMin, max, false);
							}
						});
					}
				}
				currentNodeIndex = (currentNodeIndex + 1) % nodes.length;
				loopcheck++;
			}

			if (cached == null)
				return false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public Long next() {
		return cached.get(current++);
	}

}
