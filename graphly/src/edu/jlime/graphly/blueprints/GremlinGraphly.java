package edu.jlime.graphly.blueprints;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyVertex;

public class GremlinGraphly implements Graph, Graph.Iterators,
		WrappedGraph<Graphly> {

	{
		try {
			TraversalStrategies.GlobalCache
					.registerStrategies(
							GremlinGraphly.class,
							TraversalStrategies.GlobalCache
									.getStrategies(Graph.class)
									.clone()
									.addStrategies(
											GraphlyGraphStepStrategy.instance()));
			TraversalStrategies.GlobalCache.registerStrategies(
					GremlinGraphlyVertex.class,
					TraversalStrategies.GlobalCache
							.getStrategies(Vertex.class)
							.clone()
							.addStrategies(
									GraphlyElementStepStrategy.instance()));
			TraversalStrategies.GlobalCache.registerStrategies(
					GremlinGraphlyEdge.class,
					TraversalStrategies.GlobalCache
							.getStrategies(Edge.class)
							.clone()
							.addStrategies(
									GraphlyElementStepStrategy.instance()));
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

	}

	Graphly graphly;

	public GremlinGraphly(Graphly graphly) {
		this.graphly = graphly;
	}

	@Override
	public void close() throws Exception {
		graphly.close();
	}

	@Override
	public Vertex addVertex(Object... keyValues) {
		long id = -1;
		String label = "";
		for (int i = 0; i + 1 < keyValues.length; i += 2) {
			if (keyValues[i].equals(T.id)) {
				id = (long) keyValues[i + 1];
			}
			if (keyValues[i].equals(T.label)) {
				label = (String) keyValues[i + 1];
			}
		}
		try {
			GraphlyVertex v = graphly.addVertex(id, label);
			if (v != null)
				return new GremlinGraphlyVertex(v, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public GraphComputer compute(Class... graphComputerClass) {
		return null;
	}

	@Override
	public Transaction tx() {
		return null;
	}

	@Override
	public Variables variables() {
		return null;
	}

	@Override
	public Configuration configuration() {
		return null;
	}

	@Override
	public Iterators iterators() {
		return this;
	}

	@Override
	public Iterator<Vertex> vertexIterator(Object... vertexIds) {
		return null;
	}

	@Override
	public Iterator<Edge> edgeIterator(Object... edgeIds) {
		return null;
	}

	@Override
	public Graphly getBaseGraph() {
		return graphly;
	}

	// @Override
	// public GraphTraversal<Vertex, Vertex> V(Object... vertexIds) {
	// GraphlyGraphTraversal<Vertex, Vertex> traversal = new
	// GraphlyGraphTraversal<Vertex, Vertex>(
	// GraphlyVertex.class);
	// return traversal;
	// }

}
