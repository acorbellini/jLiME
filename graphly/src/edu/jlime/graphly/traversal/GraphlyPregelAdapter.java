package edu.jlime.graphly.traversal;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.lang3.NotImplementedException;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.pregel.graph.rpc.Graph;
import gnu.trove.decorator.TLongListDecorator;
import gnu.trove.list.array.TLongArrayList;

public class GraphlyPregelAdapter implements Graph, Transferible {
	private transient Graphly g;

	public GraphlyPregelAdapter(Graphly g) {
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
	public Iterable<Long> getOutgoing(final long vertex) throws Exception {
		return new Iterable<Long>() {

			@Override
			public Iterator<Long> iterator() {
				return new ArrayIterator(g.getEdges(Dir.OUT, vertex, null));
			}
		};
	}

	@Override
	public Iterable<Long> getIncoming(final long vertex) throws Exception {
		return new Iterable<Long>() {

			@Override
			public Iterator<Long> iterator() {
				return new ArrayIterator(g.getEdges(Dir.IN, vertex, null));
			}
		};
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
	public void setRPC(RPCDispatcher rpc) throws Exception {
		this.g = (Graphly) ((JobDispatcher) rpc
				.getTarget(JobDispatcher.JOB_DISPATCHER)).getGlobal("graphly");
	}
}
