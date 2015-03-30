package edu.jlime.graphly.client;

import edu.jlime.graphly.GraphlyStoreNodeI;
import gnu.trove.list.array.TLongArrayList;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VertexIterator implements Iterator<Long> {
	ExecutorService exec = Executors.newFixedThreadPool(1);

	private Graphly g;
	private Iterator<GraphlyStoreNodeI> nodes;

	Future<TLongArrayList> fut = null;
	TLongArrayList cached = null;
	GraphlyStoreNodeI curr = null;

	int current = 0;
	private int max;

	public VertexIterator(Graphly graphly, int cached) {
		this.g = graphly;
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
					TLongArrayList obt = curr.getVertices(Long.MIN_VALUE, max,
							true);
					if (obt != null && !obt.isEmpty())
						cached = obt;
				} else {
					if (fut != null) {
						cached = fut.get();
					} else {
						cached = curr.getVertices(
								cached.get(cached.size() - 1), max, false);
					}
					if (cached.isEmpty()) {
						cached = null;
						current = 0;
					} else {
						fut = exec.submit(new Callable<TLongArrayList>() {

							@Override
							public TLongArrayList call() throws Exception {
								return curr.getVertices(
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
