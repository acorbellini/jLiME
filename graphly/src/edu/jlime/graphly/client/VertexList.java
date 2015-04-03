package edu.jlime.graphly.client;

import java.util.Iterator;

public class VertexList implements Iterable<Long> {


	private Graphly g;
	private int max;

	public VertexList(Graphly graphly, int cached) {
		this.g = graphly;
		this.max = cached;
	}

	@Override
	public Iterator<Long> iterator() {
		return new VertexIterator(g, max);
	}

}
