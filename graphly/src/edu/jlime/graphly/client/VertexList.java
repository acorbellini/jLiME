package edu.jlime.graphly.client;

import java.util.Iterator;

public class VertexList implements Iterable<Long> {


	private Graphly g;
	private int max;
	private String graph;

	public VertexList(String graph, Graphly graphly, int cached) {
		this.g = graphly;
		this.graph = graph;
		this.max = cached;
	}

	@Override
	public Iterator<Long> iterator() {
		return new VertexIterator(graph, g, max);
	}

}
