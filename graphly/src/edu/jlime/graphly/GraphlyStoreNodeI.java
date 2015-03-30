package edu.jlime.graphly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

public interface GraphlyStoreNodeI {
	@Sync
	public abstract void setProperty(long vid, String k, Object val)
			throws Exception;

	public abstract Object getProperty(long vid, String k) throws Exception;

	@Sync
	public abstract void setEdgeProperty(long v1, long v2, String k,
			Object val, String... labels) throws Exception;

	public abstract Object getEdgeProperty(long v1, long v2, String k,
			String... labels) throws Exception;

	@Sync
	public abstract void addEdge(long orig, long dest, String label,
			Object[] keyValues) throws Exception;

	public abstract List<Integer> getRanges() throws Exception;

	@Sync
	public abstract void addRange(int range) throws Exception;

	public abstract void addVertex(long id, String label) throws Exception;

	public abstract String getLabel(long id) throws Exception;

	@Sync
	public abstract void removeVertex(long id) throws Exception;

	@Sync
	public abstract void addInEdgePlaceholder(long id2, long id, String label)
			throws Exception;

	@Sync
	public abstract void addEdges(long vid, Dir dir, long[] dests)
			throws Exception;

	public abstract long[] getEdges(Dir dir, int max_edges, long[] vids)
			throws Exception;

	public abstract GraphlyCount countEdges(Dir dir, int max_edges, long[] vids)
			throws Exception;

	public abstract long getRandomEdge(long v, long[] subset, Dir d)
			throws Exception;

	@Sync
	public abstract void setProperties(String to,
			TLongObjectHashMap<Object> submap) throws Exception;

	public abstract TLongObjectHashMap<Object> getProperties(String k, int top,
			TLongArrayList value) throws Exception;

	public abstract int getEdgeCount(long vid, Dir dir, long[] at)
			throws Exception;

	@Sync
	public abstract void setTempProperties(
			HashMap<Long, Map<String, Object>> temps) throws Exception;

	@Sync
	public void commitUpdates(String[] k) throws Exception;

	public Map<Long, Map<String, Object>> getProperties(long[] array,
			String... hubKey) throws Exception;

	public abstract int getVertexCount() throws Exception;

	public abstract TLongArrayList getVertices(long from, int lenght,
			boolean includeFirst) throws Exception;

	public abstract Object getDefault(String k) throws Exception;

	@Sync
	public abstract void setDefault(String k, Object v) throws Exception;

}