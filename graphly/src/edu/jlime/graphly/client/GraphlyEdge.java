package edu.jlime.graphly.client;

public class GraphlyEdge {

	private Long from;
	private Long to;
	private GraphlyGraph g;

	public GraphlyEdge(Long id, Long id2, GraphlyGraph graphly) {
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
