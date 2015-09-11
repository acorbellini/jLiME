package edu.jlime.graphly.client;

public class Vertex {

	protected long id;

	protected Graph g;

	public Vertex(long id, Graph graphly) {
		this.id = id;
		this.g = graphly;
	}

	public Object getID() {
		return id;
	}

	public String getLabel() throws Exception {
		return g.getLabel(id);
	}

	public void remove() throws Exception {
		g.remove(id);

	}

	public Edge addEdge(String label, Vertex inVertex, Object[] keyValues) throws Exception {
		return g.addEdge(id, (Long) inVertex.getID(), label, keyValues);
	}

}
