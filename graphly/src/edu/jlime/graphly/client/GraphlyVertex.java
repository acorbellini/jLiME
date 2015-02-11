package edu.jlime.graphly.client;


public class GraphlyVertex {

	protected long id;

	protected Graphly g;

	public GraphlyVertex(long id, Graphly graphly) {
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

	public GraphlyEdge addEdge(String label, GraphlyVertex inVertex,
			Object[] keyValues) throws Exception {
		return g.addEdge(id, (Long) inVertex.getID(), label, keyValues);
	}

}
