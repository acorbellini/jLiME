package edu.jlime.pregel.worker;

import java.util.Iterator;

import edu.jlime.pregel.graph.rpc.PregelGraph;

public class GraphVertexList implements VertexList {

	private PregelGraph g;

	public GraphVertexList(PregelGraph graph) {
		this.g = graph;
	}

	@Override
	public void add(long vid) throws Exception {
		g.createVertex(vid);
	}

	@Override
	public LongIterator iterator() throws Exception {
		final Iterator<Long> it = g.vertices().iterator();
		return new LongIterator() {

			@Override
			public long next() {
				return it.next();
			}

			@Override
			public boolean hasNext() throws Exception {
				return it.hasNext();
			}
		};
	}

	@Override
	public int size() {
		try {
			return g.vertexSize();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public void flush() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete() throws Exception {
		// TODO Auto-generated method stub

	}

}
