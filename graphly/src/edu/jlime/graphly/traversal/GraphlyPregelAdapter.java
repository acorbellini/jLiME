package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.NotImplementedException;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.pregel.client.GraphConnectionFactory;
import edu.jlime.pregel.graph.rpc.Graph;
import gnu.trove.list.array.TLongArrayList;

public class GraphlyPregelAdapter implements Graph {
	private GraphlyGraph g;

	public GraphlyPregelAdapter(GraphlyGraph g) {
		this.g = g;
	}

	@Override
	public void putLink(long o, long dest) throws Exception {
		throw new NotImplementedException("PutLink is not implemented.");
	}

	@Override
	public void putOutgoing(long o, long dest) throws Exception {
		throw new NotImplementedException("PutLink is not implemented.");

	}

	@Override
	public void setVal(long v, String k, Object val) throws Exception {
		g.setProperty(v, k, val);
	}

	@Override
	public void removeOutgoing(long from, long to) throws Exception {
		throw new NotImplementedException("RemoveLink is not implemented.");

	}

	@Override
	public Object get(long v, String k) throws Exception {
		Object ret = g.getProperty(k, v);
		if (ret == null)
			ret = getDefaultValue(k);
		return ret;
	}

	@Override
	public int vertexSize() throws Exception {
		return g.getVertexCount();
	}

	@Override
	public Iterable<Long> vertices() throws Exception {
		return g.vertices();
	}

	@Override
	public int getAdyacencySize(long v) throws Exception {
		return g.getEdgesCount(Dir.BOTH, v, null);
	}

	@Override
	public TLongArrayList getOutgoing(final long vertex) throws Exception {
		return TLongArrayList.wrap(g.getEdgesFiltered(Dir.OUT, vertex, null));
	}

	@Override
	public TLongArrayList getIncoming(final long vertex) throws Exception {
		return TLongArrayList.wrap(g.getEdgesFiltered(Dir.IN, vertex, null));
	}

	@Override
	public void setDefaultValue(String k, Object d) throws Exception {
		g.setDefaultValue(k, d);
	}

	@Override
	public Object getDefaultValue(String k) throws Exception {
		return g.getDefaultValue(k);
	}

	@Override
	public int getOutgoingSize(long v) throws Exception {
		return g.getEdgesCount(Dir.OUT, v, null);
	}

	@Override
	public void putIncoming(long o, long dest) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void disable(long v) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void enableAll() throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public String print() throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void disableLink(long v, long from) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void disableOutgoing(long v, long from) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void disableIncoming(long from, long v) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public String getName() throws Exception {
		return "graphly";
	}

	@Override
	public boolean createVertex(long from) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void putOutgoing(List<long[]> value) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public void createVertices(Set<Long> set) throws Exception {
		throw new NotImplementedException("");
	}

	@Override
	public double getDouble(long v, String k) throws Exception {
		return g.getDouble(v, k);
	}

	@Override
	public void setDouble(long v, String k, double currentVal) throws Exception {
		g.setDouble(v, k, currentVal);
	}

	@Override
	public float getFloat(long v, String k) throws Exception {
		return g.getFloat(v, k);
	}

	@Override
	public void setFloat(long v, String k, float currentVal) throws Exception {
		g.setFloat(v, k, currentVal);

	}

	public static GraphConnectionFactory getFactory(String g) {
		return new GraphlyGraphConnectionFactory(g);
	}
}
