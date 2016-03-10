package edu.jlime.graphly.client;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

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
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class Graph implements Transferible {

	private static final int PRELOAD_BATCH = 10000;
	private static final long[] EMPTY_LONG_ARRAY = new long[] {};
	private String graph;
	private transient Graphly graphly;

	private transient TLongHashSet preloaded = new TLongHashSet();
	private transient TLongObjectMap<long[]> adjacencyIn = new TLongObjectHashMap<>();
	private transient TLongObjectMap<long[]> adjacencyOut = new TLongObjectHashMap<>();
	private transient InMemoryGraphProperties props = new InMemoryGraphProperties();
	private transient InMemoryGraphFloatProperties floatProps = new InMemoryGraphFloatProperties();

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
		return getEdgesMax(dir, Integer.MAX_VALUE, vids);
	}

	public long[] getEdgesMax(final Dir dir, final int max_edges, long... vids)
			throws Exception {
		if (preloaded != null && preloaded.isEmpty())
			return graphly.getEdgesMax(graph, dir, max_edges, vids);
		long[] ret = null;
		if (vids.length == 1 && preloaded != null
				&& preloaded.contains(vids[0])) {
			ret = getPreloaded(dir, vids[0], max_edges);
		} else {
			TLongHashSet remote = new TLongHashSet();
			TLongHashSet local = new TLongHashSet();
			for (long v : vids) {
				if (preloaded != null && preloaded.contains(v))
					local.add(v);
				else
					remote.add(v);
			}
			TLongHashSet aux = new TLongHashSet();
			for (long l : local.toArray()) {
				aux.addAll(getPreloaded(dir, l, max_edges));
			}
			aux.addAll(graphly.getEdgesMax(graph, dir, max_edges,
					remote.toArray()));
			ret = aux.toArray();
		}
		if (ret == null)
			ret = EMPTY_LONG_ARRAY;

		return ret;
	}

	private long[] getPreloaded(final Dir dir, long v, int max_edges) {
		long[] ret = null;
		if (dir.equals(Dir.BOTH)) {
			long[] out = adjacencyOut.get(v);
			long[] in = adjacencyIn.get(v);
			if (out == null)
				ret = in;
			else if (in == null)
				ret = out;
			else {
				TLongHashSet list = new TLongHashSet(out.length + in.length);
				list.addAll(out);
				list.addAll(in);
				ret = list.toArray();
			}
		} else if (dir.equals(Dir.IN)) {
			ret = adjacencyIn.get(v);
		} else
			ret = adjacencyOut.get(v);
		if (ret != null && ret.length > max_edges)
			ret = Arrays.copyOfRange(ret, 0, max_edges);
		return ret;
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
		if (preloaded != null && preloaded.contains(vid))
			return props.get(graph, vid, key);
		return graphly.getProperty(graph, key, vid);
	}

	public void setProperties(long vid, Map<String, Object> value)
			throws Exception {
		graphly.setProperties(graph, vid, value);
	}

	public void setProperty(long vid, String k, Object v) throws Exception {
		if (preloaded != null && preloaded.contains(vid))
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
		if (preloaded != null && preloaded.contains(vid)) {
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
		if (preloaded != null && preloaded.contains(v))
			return floatProps.get(graph, v, k);
		return this.graphly.getFloat(graph, v, k);
	}

	public void setFloat(long v, String k, float currentVal) throws Exception {
		if (preloaded != null && preloaded.contains(v))
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

	public float getFloat(long v, String k, float alt) throws Exception {
		if (preloaded != null && preloaded.contains(v)) {
			float res = floatProps.get(graph, v, k);
			if (res == InMemoryGraphFloatProperties.VALUE_NOT_FOUND)
				res = alt;
			return res;
		}
		return this.graphly.getFloat(graph, v, k, alt);
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
		preloaded = new TLongHashSet();
		adjacencyIn = new TLongObjectHashMap<>();
		adjacencyOut = new TLongObjectHashMap<>();
		props = new InMemoryGraphProperties();
		floatProps = new InMemoryGraphFloatProperties();
		{
			Logger.getLogger(Graph.class).info("Loading object properties");
			props = new InMemoryGraphProperties();
			int from = 0;
			int to = 0;
			int section = (int) Math.ceil(list.size() / (float) PRELOAD_BATCH);
			for (int i = 0; i < section; i++) {
				if (i == section - 1)
					to = list.size();
				else
					to = (i + 1) * PRELOAD_BATCH;
				from = i * PRELOAD_BATCH;
				Map<String, TLongObjectMap<Object>> map = graphly
						.getAllProperties(graph,
								list.subList(from, to).toArray());
				for (Entry<String, TLongObjectMap<Object>> e : map.entrySet()) {
					props.putAll(graph, e.getKey(), e.getValue());
				}

			}
			Logger.getLogger(Graph.class)
					.info("Finished loading " + list.size() + " properties.");

		}

		{
			Logger.getLogger(Graph.class).info("Loading float properties");
			floatProps = new InMemoryGraphFloatProperties();

			int from = 0;
			int to = 0;
			int section = (int) Math.ceil(list.size() / (float) PRELOAD_BATCH);
			for (int i = 0; i < section; i++) {
				if (i == section - 1)
					to = list.size();
				else
					to = (i + 1) * PRELOAD_BATCH;
				from = i * PRELOAD_BATCH;

				Map<String, TLongFloatMap> map = graphly.getAllFloatProperties(
						graph, list.subList(from, to).toArray());
				for (Entry<String, TLongFloatMap> e : map.entrySet()) {
					floatProps.putAll(graph, e.getKey(), e.getValue());
				}
			}
			Logger.getLogger(Graph.class)
					.info("Finished loading " + list.size() + " properties.");
		}

		{
			// if (!adjacencyLoaded) {
			if (list.size() > 0) {
				Logger.getLogger(Graph.class).info(
						"Loading adjacency of " + list.size() + " vertices.");

				int from = 0;
				int to = 0;
				int section = (int) Math
						.ceil(list.size() / (float) PRELOAD_BATCH);
				for (int i = 0; i < section; i++) {
					if (i == section - 1)
						to = list.size();
					else
						to = (i + 1) * PRELOAD_BATCH;
					from = i * PRELOAD_BATCH;

					long[] request = list.subList(from, to).toArray();

					{
						TLongObjectMap<long[]> inedges = graphly
								.getAllEdges(graph, request, Dir.IN);
						TLongObjectIterator<long[]> it = inedges.iterator();
						while (it.hasNext()) {
							it.advance();
							adjacencyIn.put(it.key(), it.value());
						}
					}
					{
						TLongObjectMap<long[]> outedges = graphly
								.getAllEdges(graph, request, Dir.OUT);
						TLongObjectIterator<long[]> itOut = outedges.iterator();
						while (itOut.hasNext()) {
							itOut.advance();
							adjacencyOut.put(itOut.key(), itOut.value());
						}
					}
				}
				Logger.getLogger(Graph.class)
						.info("Finshed loading adjacency.");
			} else
				Logger.getLogger(Graph.class).info("All vertices preloaded.");
		}

		this.preloaded.addAll(list);

	}

	public void flush(TLongArrayList list) throws Exception {
		for (String k : props.getProperties()) {
			set(k, props.get(graph, k));
		}

		for (String k : floatProps.getProperties()) {
			setFloat(k, floatProps.getAll(graph, k));
		}
	}

	public Graph createSubgraph(String string, long[] vids) throws Exception {
		graphly.createSubgraph(graph, string, vids);
		return graphly.getGraph(string);
	}

}
