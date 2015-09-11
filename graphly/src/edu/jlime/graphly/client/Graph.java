package edu.jlime.graphly.client;

import java.util.Map;
import java.util.Set;

import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.util.Gather;
import edu.jlime.graphly.util.SumFloatPropertiesGather;
import edu.jlime.graphly.util.TopGatherer;
import edu.jlime.jd.Dispatcher;
import edu.jlime.pregel.client.Pregel;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class Graph implements Transferible {

	private String graph;
	private transient Graphly graphly;

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

	public Edge addEdge(long id, long id2, String label, Object... keyValues) throws Exception {
		return graphly.addEdge(graph, id, id2, label, keyValues);
	}

	public long[] getEdges(Dir dir, long... vids) throws Exception {
		return graphly.getEdges(graph, dir, vids);
	}

	public long[] getEdgesMax(final Dir dir, final int max_edges, long... vids) throws Exception {
		return graphly.getEdgesMax(graph, dir, max_edges, vids);
	}

	public void addEdges(long vid, Dir dir, long[] dests) throws Exception {
		graphly.addEdges(graph, vid, dir, dests);
	}

	public Count countEdges(final Dir dir, final int max_edges, long[] keys, float[] values, long[] toFilter)
			throws Exception {
		return graphly.countEdges(graph, dir, max_edges, keys, values, toFilter);
	}

	public long getRandomEdge(long before, long[] subset, Dir d) throws Exception {
		return graphly.getRandomEdge(graph, before, subset, d);
	}

	public Object getProperty(String key, long vid) throws Exception {
		return graphly.getProperty(graph, key, vid);
	}

	public void setProperties(long vid, Map<String, Object> value) throws Exception {
		graphly.setProperties(graph, vid, value);
	}

	public void setProperty(long vid, String k, Object v) throws Exception {
		graphly.setProperty(graph, vid, k, v);
	}

	public Object getProperty(long vid, String k, Object alt) throws Exception {
		return graphly.getProperty(graph, vid, k, alt);
	}

	public TLongObjectHashMap<Object> collect(String k, int top, long[] vids) throws Exception {
		return graphly.collect(graph, k, top, vids);
	}

	public void set(String to, TLongObjectHashMap<Object> map) throws Exception {
		graphly.set(graph, to, map);
	}

	public int getEdgesCount(Dir dir, long vid, TLongHashSet at) throws Exception {
		return graphly.getEdgesCount(graph, dir, vid, at);
	}

	public SubGraph getSubGraph(String k) {
		return graphly.getSubGraph(graph, k);
	}

	public SubGraph getSubGraph(String k, long[] all) {
		return graphly.getSubGraph(graph, k, all);
	}

	public long[] getEdgesFiltered(Dir in, long vid, TLongHashSet all) {
		return graphly.getEdgesFiltered(graph, in, vid, all);
	}

	public Map<Long, Map<String, Object>> getProperties(long[] array, String... k) throws Exception {
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

	public void setDouble(long v, String k, double currentVal) throws Exception {
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

	public void setTempProperties(long[] before, Map<Long, Map<String, Object>> temps) throws InterruptedException {
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
		return this.graphly.getFloat(graph, v, k);
	}

	public void setFloat(long v, String k, float currentVal) throws Exception {
		this.graphly.setFloat(graph, v, k, currentVal);
	}

	public void setDefaultFloat(String string, float f) throws Exception {
		this.graphly.setDefaultFloat(graph, string, f);
	}

	public <T> GatherResult<T> gather(Gather<T> g) throws Exception {
		return new GatherResult(this.graphly.gather(graph, g));
	}

	public Set<Pair<Long, Float>> topFloat(String string, int i) throws Exception {
		return this.gather(new TopGatherer(string, i)).merge(new TopMerger(i));
	}

	public Float sumFloat(String string) throws Exception {
		return this.gather(new SumFloatPropertiesGather(string)).merge(new SumMerger());
	}

	public Float quadSumFloat(String string) throws Exception {
		return this.gather(new QuadSumFloatPropertiesGather(string)).merge(new SumMerger());
	}

	public float getDefaultFloat(String prop) throws Exception {
		return this.graphly.getDefaultFloat(graph, prop);
	}

	public Traversal v(TLongHashSet ids) {
		return graphly.v(graph, ids);

	}

	public Set<Pair<Long, Float>> topFloat(String string, int i, TLongHashSet vertices) throws Exception {
		return this.gather(new TopGatherer(string, i, vertices)).merge(new TopMerger(i));
	}

	public float sumFloat(String string, TLongHashSet vertices) throws Exception {
		return this.gather(new SumFloatPropertiesGather(string, vertices)).merge(new SumMerger());
	}

	public float quadSumFloat(String string, TLongHashSet vertices) throws Exception {
		return this.gather(new QuadSumFloatPropertiesGather(string, vertices)).merge(new SumMerger());
	}

	public void setTempFloats(String k, boolean add, TLongFloatHashMap v) throws Exception {
		graphly.setTempFloats(graph, k, add, v);

	}

	public void commitFloatUpdates(String... props) throws Exception {
		this.graphly.commitFloatUpdates(graph, props);

	}

	public void updateFloatProperty(String prop, DivideUpdateProperty upd) throws Exception {
		this.graphly.updateFloatProperty(graph, prop, upd);
	}

	public float getFloat(long in, String hubKey, float alt) throws Exception {
		return this.graphly.getFloat(graph, in, hubKey, alt);
	}

	public void setFloat(String k, TLongFloatHashMap auth2) {
		this.graphly.setFloat(graph, k, auth2);
	}

	public TLongFloatHashMap getFloats(String k) throws Exception {
		return getFloats(k, null);
	}

	public TLongFloatHashMap getFloats(String k, TLongHashSet vertices) throws Exception {
		return this.gather(new FloatGather(k, vertices)).merge(new FloatMerger());
	}

	public void setProperty(TLongHashSet vertices, String k, String val) {
		this.graphly.setProperty(graph, vertices, k, val);
	}
}
