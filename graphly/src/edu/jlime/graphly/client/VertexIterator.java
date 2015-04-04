package edu.jlime.graphly.client;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.graphly.GraphlyStoreNodeI;
import gnu.trove.list.array.TLongArrayList;

public class VertexIterator implements Iterator<Long> {
	ExecutorService exec = Executors.newFixedThreadPool(1);

	private Graphly g;
	private Iterator<GraphlyStoreNodeI> nodes;

	Future<TLongArrayList> fut = null;
	TLongArrayList cached = null;
	GraphlyStoreNodeI curr = null;

	int current = 0;
	private int max;

	private String gID;

	public VertexIterator(String graph, Graphly graphly, int cached) {
		this.g = graphly;
		this.gID = graph;
		this.nodes = g.mgr.getAll().iterator();
		this.max = cached;
	}

	@Override
	public boolean hasNext() {
		try {
			while (cached == null || current >= cached.size()) {
				if (cached == null) {
					if (!nodes.hasNext())
						return false;
					curr = nodes.next();
					TLongArrayList obt = curr.getVertices(gID, Long.MIN_VALUE,
							max, true);
					if (obt != null && !obt.isEmpty())
						cached = obt;
				} else {
					if (fut != null) {
						cached = fut.get();
					} else {
						cached = curr.getVertices(gID,
								cached.get(cached.size() - 1), max, false);
					}
					if (cached.isEmpty()) {
						cached = null;
						current = 0;
					} else {
						fut = exec.submit(new Callable<TLongArrayList>() {

							@Override
							public TLongArrayList call() throws Exception {
								return curr.getVertices(gID,
										cached.get(cached.size() - 1), max,
										false);
							}

						});
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public Long next() {
		return cached.get(current++);
	}

}
