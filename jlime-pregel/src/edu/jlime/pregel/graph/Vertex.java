package edu.jlime.pregel.graph;

public class Vertex {
	private PregelGraph graph;
	private int id;

	public Vertex(PregelGraph graph, int id) {
		this.graph = graph;
		this.id = id;
	}

	public void link(Vertex v) {
		graph.putLink(this, v);
	}

	public int getId() {
		return id;
	}
}