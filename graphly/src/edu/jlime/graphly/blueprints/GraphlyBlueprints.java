package edu.jlime.graphly.blueprints;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class GraphlyBlueprints implements Graph {

	Graphly graphly;

	public GraphlyBlueprints(Graphly graphly) {
		this.graphly = graphly;
	}

	@Override
	public Features getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vertex addVertex(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vertex getVertex(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeVertex(Vertex vertex) {
		// TODO Auto-generated method stub
	}

	@Override
	public Iterable<Vertex> getVertices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex,
			String label) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Edge getEdge(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeEdge(Edge edge) {
		// TODO Auto-generated method stub
	}

	@Override
	public Iterable<Edge> getEdges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GraphQuery query() {
		return null;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
	}

}
