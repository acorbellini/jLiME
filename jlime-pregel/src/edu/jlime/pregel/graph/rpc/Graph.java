package edu.jlime.pregel.graph.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import edu.jlime.core.rpc.Sync;
import gnu.trove.list.array.TLongArrayList;

public interface Graph extends Serializable {

	@Sync
	public abstract void putLink(long o, long dest) throws Exception;

	@Sync
	public abstract void putOutgoing(long o, long dest) throws Exception;

	@Sync
	public abstract void setVal(long v, String k, Object val) throws Exception;

	@Sync
	public abstract void removeOutgoing(long from, long to) throws Exception;

	public abstract Object get(long v, String k) throws Exception;

	public abstract int vertexSize() throws Exception;

	public abstract Iterable<Long> vertices() throws Exception;

	public abstract int getAdyacencySize(long v) throws Exception;

	public abstract TLongArrayList getOutgoing(long vertex) throws Exception;

	public abstract TLongArrayList getIncoming(long v) throws Exception;

	@Sync
	public abstract void setDefaultValue(String k, Object d) throws Exception;

	public abstract Object getDefaultValue(String k) throws Exception;

	public abstract int getOutgoingSize(long v) throws Exception;

	@Sync
	public abstract void putIncoming(long o, long dest) throws Exception;

	@Sync
	public abstract void disable(long v) throws Exception;

	@Sync
	public abstract void enableAll() throws Exception;

	public abstract String print() throws Exception;

	@Sync
	public abstract void disableLink(long v, long from) throws Exception;

	@Sync
	public abstract void disableOutgoing(long v, long from) throws Exception;

	@Sync
	public abstract void disableIncoming(long from, long v) throws Exception;

	public String getName() throws Exception;

	public abstract boolean createVertex(long from) throws Exception;

	@Sync
	public abstract void putOutgoing(List<long[]> value) throws Exception;

	@Sync
	public abstract void createVertices(Set<Long> set) throws Exception;

	public abstract double getDouble(long v, String string) throws Exception;

	public abstract void setDouble(long v, String string, double currentVal)
			throws Exception;

}