package edu.jlime.graphly.client;

public class GraphlyEdge {

	private Long from;
	private Long to;
	private Graphly g;

	public GraphlyEdge(Long id, Long id2, Graphly graphly) {
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
