package edu.jlime.graphly.client;

public class Edge {

	private Long from;
	private Long to;
	private Graph g;

	public Edge(Long id, Long id2, Graph graphly) {
		this.from = id;
		this.to = id2;
		this.g = graphly;
	}

	public Long getFrom() {
		return from;
	}

	public Long getTo() {
		return to;
	}

}
