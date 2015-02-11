package edu.jlime.graphly.blueprints;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;

import edu.jlime.graphly.client.GraphlyEdge;

public class GremlinGraphlyEdge implements Edge {

	private GraphlyEdge e;
	private GremlinGraphly g;

	public GremlinGraphlyEdge(GraphlyEdge e, GremlinGraphly g) {
		this.e = e;
		this.g = g;
	}

	@Override
	public Object id() {
		return e.getFrom() + "" + e.getTo();
	}

	@Override
	public String label() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph graph() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V> Property<V> property(String key, V value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterators iterators() {
		// TODO Auto-generated method stub
		return null;
	}

}
