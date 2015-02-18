package edu.jlime.graphly.blueprints;

import java.util.Iterator;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import edu.jlime.graphly.client.GraphlyEdge;
import edu.jlime.graphly.client.GraphlyVertex;

public class GremlinGraphlyVertex implements Vertex, Vertex.Iterators {

	private GraphlyVertex v;
	private GremlinGraphly g;

	public GremlinGraphlyVertex(GraphlyVertex v, GremlinGraphly g) {
		this.v = v;
		this.g = g;
	}

	@Override
	public Object id() {
		return v.getID();
	}

	@Override
	public String label() {
		try {
			return v.getLabel();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Graph graph() {
		return g;
	}

	@Override
	public void remove() {
		try {
			v.remove();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		GraphlyEdge e;
		try {
			e = v.addEdge(label, (GraphlyVertex) inVertex, keyValues);
			return new GremlinGraphlyEdge(e, g);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return null;

	}

	@Override
	public <V> VertexProperty<V> property(String key, V value) {
		return null;
	}

	@Override
	public Iterators iterators() {
		return this;
	}

	@Override
	public Iterator<Edge> edgeIterator(Direction direction,
			String... edgeLabels) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Vertex> vertexIterator(Direction direction,
			String... edgeLabels) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V> Iterator<VertexProperty<V>> propertyIterator(
			String... propertyKeys) {
		// TODO Auto-generated method stub
		return null;
	}

}
