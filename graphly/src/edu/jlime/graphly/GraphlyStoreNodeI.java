package edu.jlime.graphly;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public interface GraphlyStoreNodeI {
	@Sync
	public abstract void setProperty(Long vid, String k, Object val)
			throws Exception;

	public abstract Object getProperty(Long vid, String k) throws Exception;

	@Sync
	public abstract void setEdgeProperty(Long v1, Long v2, String k,
			Object val, String... labels) throws Exception;

	public abstract Object getEdgeProperty(Long v1, Long v2, String k,
			String... labels) throws Exception;

	@Sync
	public abstract void addEdge(Long orig, Long dest, String label,
			Object[] keyValues) throws Exception;

	public abstract List<Integer> getRanges() throws Exception;

	@Sync
	public abstract void addRange(Integer range) throws Exception;

	public abstract boolean addVertex(Long id, String label) throws Exception;

	public abstract String getLabel(Long id) throws Exception;

	@Sync
	public abstract void removeVertex(Long id) throws Exception;

	@Sync
	public abstract void addInEdgePlaceholder(Long id2, Long id, String label)
			throws Exception;

	@Sync
	public abstract void addEdges(Long vid, Dir dir, long[] dests)
			throws Exception;

	public abstract long[] getEdges(Dir dir, Integer max_edges, long[] vids)
			throws Exception;

	@Cache
	public abstract Peer getJobAddress() throws Exception;

	public abstract GraphlyCount countEdges(Dir dir, Integer max_edges,
			long[] vids) throws Exception;

	public abstract Long getRandomEdge(Long v, long[] subset, Dir d)
			throws Exception;

	@Sync
	public abstract void setProperties(String to,
			TLongObjectHashMap<Object> submap) throws Exception;

	public abstract TLongObjectHashMap<Object> getProperties(String k,
			Integer top, TLongArrayList value) throws Exception;

	public abstract int getEdgeCount(Long vid, Dir dir, long[] at)
			throws Exception;

}