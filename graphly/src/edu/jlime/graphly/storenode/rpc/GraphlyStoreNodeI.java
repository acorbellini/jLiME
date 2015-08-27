package edu.jlime.graphly.storenode.rpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.jlime.core.rpc.Sync;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.Gather;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

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
			int max_edges, TLongFloatMap data, TLongHashSet toFilter)
			throws Exception;

	public abstract long getRandomEdge(String graph, long v, long[] subset,
			Dir d) throws Exception;

	@Sync
	public abstract void setProperties(String graph, String to,
			TLongObjectHashMap<Object> submap) throws Exception;

	public abstract TLongObjectHashMap<Object> getProperties(String graph,
			String k, int top, TLongArrayList value) throws Exception;

	public abstract int getEdgeCount(String graph, long vid, Dir dir,
			TLongHashSet at) throws Exception;

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

	@Sync
	public abstract void setDouble(String graph, long v, String k,
			double currentVal) throws Exception;

	@Sync
	public abstract void setDefaultDouble(String graph, String k, double v)
			throws Exception;

	public abstract double getDefaultDouble(String graph, String k)
			throws Exception;

	public abstract Set<String> getGraphs() throws Exception;

	public abstract float getFloat(String graph, long v, String k)
			throws Exception;

	@Sync
	public abstract void setFloat(String graph, long v, String k,
			float currentVal) throws Exception;

	@Sync
	public void setDefaultFloat(String graph, String k, float v)
			throws Exception;

	public float getDefaultFloat(String graph, String k) throws Exception;

	public Object gather(String graph, Gather<?> g) throws Exception;

	@Sync
	public void setTempFloats(String graph, String k, boolean add,
			TLongFloatHashMap subProp) throws Exception;

	@Sync
	public void commitFloatUpdates(String graph, String... props)
			throws Exception;

	@Sync
	public void updateFloatProperty(String graph, String prop,
			DivideUpdateProperty upd) throws Exception;

	@Sync
	public abstract float getFloat(String graph, long v, String k, float alt)
			throws Exception;

	@Sync
	public abstract void setFloats(String graph, String k,
			TLongFloatHashMap subProp) throws Exception;

	@Sync
	public abstract void setProperty(String graph, String k, String val,
			TLongArrayList value) throws Exception;
}