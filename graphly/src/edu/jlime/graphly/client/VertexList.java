package edu.jlime.graphly.client;

import java.util.Iterator;

public class VertexList implements Iterable<Long> {

	private GraphlyClient g;
	private int max;
	private String graph;

	public VertexList(String graph, GraphlyClient graphly, int cached) {
		this.g = graphly;
		this.graph = graph;
		this.max = cached;
	}

	@Override
	public Iterator<Long> iterator() {
		return new VertexIterator(graph, g.mgr.getAll(), max);
	}

}
