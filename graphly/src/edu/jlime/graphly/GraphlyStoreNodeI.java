package edu.jlime.graphly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.jlime.core.rpc.Sync;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

public interface GraphlyStoreNodeI {
	@Sync
	public abstract void setProperty(String graph, long vid, String k,
			Object val) throws Exception;

	public abstract Object getProperty(String graph, long vid, String k)
			throws Exception;

	@Sync
	public abstract void setEdgeProperty(String graph, long v1, long v2,
			String k, Object val, String... labels) throws Exception;

	public abstract Object getEdgeProperty(String graph, long v1, long v2,
			String k, String... labels) throws Exception;

	@Sync
	public abstract void addEdge(String graph, long orig, long dest,
			String label, Object[] keyValues) throws Exception;

	public abstract List<Integer> getRanges() throws Exception;

	@Sync
	public abstract void addRange(int range) throws Exception;

	public abstract void addVertex(String graph, long id, String label)
			throws Exception;

	public abstract String getLabel(String graph, long id) throws Exception;

	@Sync
	public abstract void removeVertex(String graph, long id) throws Exception;

	@Sync
	public abstract void addInEdgePlaceholder(String graph, long id2, long id,
			String label) throws Exception;

	@Sync
	public abstract void addEdges(String graph, long vid, Dir dir, long[] dests)
			throws Exception;

	public abstract long[] getEdges(String graph, Dir dir, int max_edges,
			long[] vids) throws Exception;

	public abstract GraphlyCount countEdges(String graph, Dir dir,
			int max_edges, long[] vids) throws Exception;

	public abstract long getRandomEdge(String graph, long v, long[] subset,
			Dir d) throws Exception;

	@Sync
	public abstract void setProperties(String graph, String to,
			TLongObjectHashMap<Object> submap) throws Exception;

	public abstract TLongObjectHashMap<Object> getProperties(String graph,
			String k, int top, TLongArrayList value) throws Exception;

	public abstract int getEdgeCount(String graph, long vid, Dir dir, long[] at)
			throws Exception;

	@Sync
	public abstract void setTempProperties(String graph,
			HashMap<Long, Map<String, Object>> temps) throws Exception;

	@Sync
	public void commitUpdates(String graph, String[] k) throws Exception;

	public Map<Long, Map<String, Object>> getProperties(String graph,
			long[] array, String... hubKey) throws Exception;

	public abstract int getVertexCount(String graph) throws Exception;

	public abstract TLongArrayList getVertices(String graph, long from,
			int lenght, boolean includeFirst) throws Exception;

	public abstract Object getDefault(String graph, String k) throws Exception;

	@Sync
	public abstract void setDefault(String graph, String k, Object v)
			throws Exception;

	public abstract double getDouble(String graph, long v, String k)
			throws Exception;

	public abstract void setDouble(String graph, long v, String k,
			double currentVal) throws Exception;

	public abstract void setDefaultDouble(String graph, String k, double v)
			throws Exception;

	public abstract double getDefaultDouble(String graph, String k)
			throws Exception;

	public abstract Set<String> getGraphs() throws Exception;

}