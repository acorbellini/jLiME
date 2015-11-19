package edu.jlime.graphly.client;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.storenode.properties.InMemoryGraphFloatProperties;
import edu.jlime.graphly.storenode.properties.InMemoryGraphProperties;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.util.Gather;
import edu.jlime.graphly.util.SumFloatPropertiesGather;
import edu.jlime.graphly.util.TopGatherer;
import edu.jlime.jd.Dispatcher;
import edu.jlime.pregel.client.Pregel;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class Graph implements Transferible {

	private static final float PRELOAD_BATCH = 1000f;
	private String graph;
	private transient Graphly graphly;

	private InMemoryGraphProperties props = new InMemoryGraphProperties();
	private InMemoryGraphFloatProperties floatProps = new InMemoryGraphFloatProperties();
	private TLongHashSet preloaded = new TLongHashSet();
	private TLongObjectMap<long[]> adjacencyIn = new TLongObjectHashMap<>();
	private TLongObjectMap<long[]> adjacencyOut = new TLongObjectHashMap<>();

	public Graph(Graphly graphly, String graphName) {
		this.graph = graphName;
		this.graphly = graphly;
	}

	public Traversal v(long... id) {
		return graphly.v(graph, id);
	}

	public Vertex addVertex(long id, String label) throws Exception {
		graphly.addVertex(graph, id, label);
		return graphly.getVertex(graph, id);
	}

	public String getLabel(long id) throws Exception {
		return graphly.getLabel(graph, id);
	}

	public void remove(long id) throws Exception {
		graphly.remove(graph, id);
	}

	public Edge addEdge(long id, long id2, String label, Object... keyValues)
			throws Exception {
		return graphly.addEdge(graph, id, id2, label, keyValues);
	}

	public long[] getEdges(Dir dir, long... vids) throws Exception {
		if (vids.length == 1 && preloaded.contains(vids[0]))
			return dir == Dir.IN ? adjacencyIn.get(vids[0])
					: adjacencyOut.get(vids[0]);
		boolean hasPre = false;
		for (long v : vids)
			if (preloaded.contains(v)) {
				hasPre = true;
				break;
			}
		if (!hasPre)
			return graphly.getEdges(graph, dir, vids);
		else {

			TLongHashSet ret = new TLongHashSet();
			TLongHashSet local = new TLongHashSet();
			for (long v : vids) {
				if (preloaded.contains(v))
					ret.addAll(dir == Dir.IN ? adjacencyIn.get(v)
							: adjacencyOut.get(v));
				else
					local.add(v);
			}
			ret.addAll(graphly.getEdges(graph, dir, local.toArray()));
			return ret.toArray();
		}
	}

	public long[] getEdgesMax(final Dir dir, final int max_edges, long... vids)
			throws Exception {
		return graphly.getEdgesMax(graph, dir, max_edges, vids);
	}

	public void addEdges(long vid, Dir dir, long[] dests) throws Exception {
		graphly.addEdges(graph, vid, dir, dests);
	}

	public Count countEdges(final Dir dir, final int max_edges, long[] keys,
			float[] values, long[] toFilter) throws Exception {
		return graphly.countEdges(graph, dir, max_edges, keys, values,
				toFilter);
	}

	public long getRandomEdge(long before, long[] subset, Dir d)
			throws Exception {
		return graphly.getRandomEdge(graph, before, subset, d);
	}

	public Object getProperty(String key, long vid) throws Exception {
		if (preloaded.contains(vid))
			return props.get(graph, vid, key);
		return graphly.getProperty(graph, key, vid);
	}

	public void setProperties(long vid, Map<String, Object> value)
			throws Exception {
		graphly.setProperties(graph, vid, value);
	}

	public void setProperty(long vid, String k, Object v) throws Exception {
		if (preloaded.contains(vid))
			props.put(graph, vid, k, v);
		else
			graphly.setProperty(graph, vid, k, v);
	}

	public Object getProperty(long vid, String k, Object alt) throws Exception {
		return graphly.getProperty(graph, vid, k, alt);
	}

	public TLongObjectHashMap<Object> collect(String k, int top, long[] vids)
			throws Exception {
		return graphly.collect(graph, k, top, vids);
	}

	public void set(String key, TLongObjectMap<Object> map) throws Exception {
		graphly.set(graph, key, map);
	}

	public int getEdgesCount(Dir dir, long vid, TLongHashSet at)
			throws Exception {
		return graphly.getEdgesCount(graph, dir, vid, at);
	}

	public SubGraph getSubGraph(String k) {
		return graphly.getSubGraph(graph, k);
	}

	public SubGraph getSubGraph(String k, long[] all) {
		return graphly.getSubGraph(graph, k, all);
	}

	public long[] getEdgesFiltered(Dir in, long vid, TLongHashSet all)
			throws Exception {
		if (preloaded.contains(vid)) {
			long[] edges = getEdges(in, vid);

			if (edges != null)
				if (all == null || all.size() == 0)
					return edges;
				else {
					TLongHashSet ret = new TLongHashSet();
					for (long l : edges) {
						if (all.contains(l))
							ret.add(l);
					}
					return ret.toArray();
				}
			else
				System.out.println("Edges is null for " + vid);
			return new long[] {};
		} else
			return graphly.getEdgesFiltered(graph, in, vid, all);
	}

	public Map<Long, Map<String, Object>> getProperties(long[] array,
			String... k) throws Exception {
		return graphly.getProperties(graph, array, k);
	}

	public int getVertexCount() throws Exception {
		return graphly.getVertexCount(graph);
	}

	public VertexList vertices() throws Exception {
		return graphly.vertices(graph);
	}

	public void setDefaultValue(String k, Object v) throws Exception {
		graphly.setDefaultValue(graph, k, v);
	}

	public Object getDefaultValue(String k) throws Exception {
		return graphly.getDefaultValue(graph, k);
	}

	public double getDouble(long v, String k) throws Exception {
		return graphly.getDouble(graph, v, k);
	}

	public void setDefaultDouble(String k, double v) throws Exception {
		graphly.setDefaultDouble(graph, k, v);
	}

	public void setDouble(long v, String k, double currentVal)
			throws Exception {
		graphly.setDouble(graph, v, k, currentVal);
	}

	public Graphly getGraphly() {
		return graphly;
	}

	public String getGraph() {
		return graph;
	}

	@Override
	public void setRPC(RPC rpc) throws Exception {
		Dispatcher jd = (Dispatcher) rpc.getTarget("JD");
		this.graphly = (Graphly) jd.getGlobal("graphly");
	}

	public void setTempProperties(long[] before,
			Map<Long, Map<String, Object>> temps) throws InterruptedException {
		this.graphly.setTempProperties(graph, before, temps);

	}

	public Dispatcher getJobClient() {
		return this.graphly.getJobClient();
	}

	public void commitUpdates(String... k) throws Exception {
		this.graphly.commitUpdates(graph, k);

	}

	public RPC getRpc() {
		return this.graphly.getRpc();
	}

	public Pregel getPregeClient() {
		return this.graphly.getPregeClient();
	}

	public ConsistentHashing getHash() {
		return this.graphly.getHash();
	}

	public float getFloat(long v, String k) throws Exception {
		if (preloaded.contains(v))
			return floatProps.get(graph, v, k);
		return this.graphly.getFloat(graph, v, k);
	}

	public void setFloat(long v, String k, float currentVal) throws Exception {
		if (preloaded.contains(v))
			floatProps.put(graph, v, k, currentVal);
		else
			this.graphly.setFloat(graph, v, k, currentVal);
	}

	public void setDefaultFloat(String string, float f) throws Exception {
		this.graphly.setDefaultFloat(graph, string, f);
	}

	public <T> GatherResult<T> gather(Gather<T> g) throws Exception {
		return new GatherResult(this.graphly.gather(graph, g));
	}

	public Set<Pair<Long, Float>> topFloat(String string, int i)
			throws Exception {
		return this.gather(new TopGatherer(string, i)).merge(new TopMerger(i));
	}

	public Float sumFloat(String string) throws Exception {
		return this.gather(new SumFloatPropertiesGather(string))
				.merge(new SumMerger());
	}

	public Float quadSumFloat(String string) throws Exception {
		return this.gather(new QuadSumFloatPropertiesGather(string))
				.merge(new SumMerger());
	}

	public float getDefaultFloat(String prop) throws Exception {
		return this.graphly.getDefaultFloat(graph, prop);
	}

	public Traversal v(TLongHashSet ids) {
		return graphly.v(graph, ids);

	}

	public Set<Pair<Long, Float>> topFloat(String string, int i,
			TLongHashSet vertices) throws Exception {
		return this.gather(new TopGatherer(string, i, vertices))
				.merge(new TopMerger(i));
	}

	public float sumFloat(String string, TLongHashSet vertices)
			throws Exception {
		return this.gather(new SumFloatPropertiesGather(string, vertices))
				.merge(new SumMerger());
	}

	public float quadSumFloat(String string, TLongHashSet vertices)
			throws Exception {
		return this.gather(new QuadSumFloatPropertiesGather(string, vertices))
				.merge(new SumMerger());
	}

	public void setTempFloats(String k, boolean add, TLongFloatMap v)
			throws Exception {
		graphly.setTempFloats(graph, k, add, v);

	}

	public void commitFloatUpdates(String... props) throws Exception {
		this.graphly.commitFloatUpdates(graph, props);

	}

	public void updateFloatProperty(String prop, DivideUpdateProperty upd)
			throws Exception {
		this.graphly.updateFloatProperty(graph, prop, upd);
	}

	public float getFloat(long in, String hubKey, float alt) throws Exception {
		return this.graphly.getFloat(graph, in, hubKey, alt);
	}

	public void setFloat(String k, TLongFloatMap auth2) throws Exception {
		this.graphly.setFloat(graph, k, auth2);
	}

	public TLongFloatMap getFloats(String k) throws Exception {
		return getFloats(k, null);
	}

	public TLongFloatMap getFloats(String k, TLongHashSet vertices)
			throws Exception {
		return this.gather(new FloatGather(k, vertices))
				.merge(new FloatMerger());
	}

	public void setProperty(TLongHashSet vertices, String k, String val) {
		this.graphly.setProperty(graph, vertices, k, val);
	}

	public void preload(TLongArrayList list) throws Exception {
		this.preloaded = new TLongHashSet(list);
		long[] array = list.toArray();
		{
			props = new InMemoryGraphProperties();
			int from = 0;
			int to = 0;
			int section = (int) Math.ceil(array.length / PRELOAD_BATCH);
			for (int i = 0; i < section; i++) {
				if (i == section - 1)
					to = array.length;
				else
					to = (i + 1) * section;
				from = i * section;
				Map<String, TLongObjectMap<Object>> map = graphly
						.getAllProperties(graph,
								Arrays.copyOfRange(array, from, to));
				for (Entry<String, TLongObjectMap<Object>> e : map.entrySet()) {
					props.putAll(graph, e.getKey(), e.getValue());
				}

			}

		}
		{
			floatProps = new InMemoryGraphFloatProperties();

			int from = 0;
			int to = 0;
			int section = (int) Math.ceil(array.length / PRELOAD_BATCH);
			for (int i = 0; i < section; i++) {
				if (i == section - 1)
					to = array.length;
				else
					to = (i + 1) * section;
				from = i * section;

				Map<String, TLongFloatMap> map = graphly.getAllFloatProperties(
						graph, Arrays.copyOfRange(array, from, to));
				for (Entry<String, TLongFloatMap> e : map.entrySet()) {
					floatProps.putAll(graph, e.getKey(), e.getValue());
				}
			}
		}

		{
			// if (!adjacencyLoaded) {
			long[] toLoad = null;
			if (adjacencyIn.isEmpty() && adjacencyOut.isEmpty())
				toLoad = array;
			else {
				TLongArrayList curr = new TLongArrayList();
				for (long v : array) {
					if (!adjacencyIn.containsKey(v)
							|| !adjacencyOut.containsKey(v))
						curr.add(v);
				}
				toLoad = curr.toArray();
			}
			if (toLoad.length > 0) {
				int from = 0;
				int to = 0;
				int section = (int) Math.ceil(toLoad.length / PRELOAD_BATCH);
				for (int i = 0; i < section; i++) {
					if (i == section - 1)
						to = toLoad.length;
					else
						to = (i + 1) * section;
					from = i * section;

					adjacencyIn.putAll(graphly.getAllEdges(graph,
							Arrays.copyOfRange(toLoad, from, to), Dir.IN));
					adjacencyOut.putAll(graphly.getAllEdges(graph,
							Arrays.copyOfRange(toLoad, from, to), Dir.OUT));
				}
			}
		}

	}

	public void flush(TLongArrayList list) throws Exception {
		for (String k : props.getProperties()) {
			set(k, props.get(graph, k));
		}

		for (String k : floatProps.getProperties()) {
			setFloat(k, floatProps.getAll(graph, k));
		}
	}
}
