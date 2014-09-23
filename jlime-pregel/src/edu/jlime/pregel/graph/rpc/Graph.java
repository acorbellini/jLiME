package edu.jlime.pregel.graph.rpc;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import edu.jlime.core.rpc.Sync;

public interface Graph extends Serializable {

	@Sync
	public abstract void putLink(Long o, Long dest) throws Exception;

	@Sync
	public abstract void putOutgoing(Long o, Long dest) throws Exception;

	@Sync
	public abstract void setVal(Long v, String k, Object val) throws Exception;

	@Sync
	public abstract void removeOutgoing(Long from, Long to) throws Exception;

	public abstract Object get(Long v, String k) throws Exception;

	public abstract int vertexSize() throws Exception;

	public abstract Collection<Long> vertices() throws Exception;

	public abstract int getAdyacencySize(Long v) throws Exception;

	public abstract Collection<Long> getOutgoing(Long vertex) throws Exception;

	public abstract Collection<Long> getIncoming(Long v) throws Exception;

	@Sync
	public abstract void setDefaultValue(String k, Object d) throws Exception;

	public abstract Object getDefaultValue(String k) throws Exception;

	public abstract int getOutgoingSize(Long v) throws Exception;

	@Sync
	public abstract void putIncoming(Long o, Long dest) throws Exception;

	@Sync
	public abstract void disable(Long v) throws Exception;

	@Sync
	public abstract void enableAll() throws Exception;

	public abstract String print() throws Exception;

	@Sync
	public abstract void disableLink(Long v, Long from) throws Exception;

	@Sync
	public abstract void disableOutgoing(Long v, Long from) throws Exception;

	@Sync
	public abstract void disableIncoming(Long from, Long v) throws Exception;

	public String getName() throws Exception;

	public abstract boolean createVertex(Long from) throws Exception;

	@Sync
	public abstract void putOutgoing(List<Long[]> value) throws Exception;

	@Sync
	public abstract void createVertices(Set<Long> set) throws Exception;

}