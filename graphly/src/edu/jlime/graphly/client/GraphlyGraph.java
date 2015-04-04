package edu.jlime.graphly.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.TreeMultimap;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.GraphlyCount;
import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.pregel.client.PregelClient;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyGraph implements Transferible {

	private String graph;
	private transient Graphly graphly;

	public GraphlyGraph(Graphly graphly, String graphName) {
		this.graph = graphName;
		this.graphly = graphly;
	}

	public GraphlyTraversal v(long... id) {
		return graphly.v(graph, id);
	}

	public GraphlyVertex addVertex(long id, String label) throws Exception {
		graphly.addVertex(graph, id, label);
		return graphly.getVertex(graph, id);
	}

	public String getLabel(long id) throws Exception {
		return graphly.getLabel(graph, id);
	}

	public void remove(long id) throws Exception {
		graphly.remove(graph, id);
	}

	public GraphlyEdge addEdge(long id, long id2, String label,
			Object... keyValues) throws Exception {
		return graphly.addEdge(graph, id, id2, label, keyValues);
	}

	public long[] getEdges(Dir dir, long... vids) throws Exception {
		return graphly.getEdges(graph, dir, -1, vids);
	}

	public long[] getEdges(final Dir dir, final int max_edges, long... vids)
			throws Exception {
		return graphly.getEdges(graph, dir, max_edges, vids);
	}

	public void addEdges(long vid, Dir dir, long[] dests) throws Exception {
		graphly.addEdges(graph, vid, dir, dests);
	}

	public GraphlyCount countEdges(final Dir dir, final int max_edges,
			long[] vids) throws Exception {
		return graphly.countEdges(graph, dir, max_edges, vids);
	}

	public long getRandomEdge(long before, long[] subset, Dir d)
			throws Exception {
		return graphly.getRandomEdge(graph, before, subset, d);
	}

	public Object getProperty(String key, long vid) throws Exception {
		return graphly.getProperty(graph, key, vid);
	}

	public void setProperties(long vid, Map<String, Object> value)
			throws Exception {
		graphly.setProperties(graph, vid, value);
	}

	public void setProperty(long vid, String k, Object v) throws Exception {
		graphly.setProperty(graph, vid, k, v);
	}

	public Object getProperty(long vid, String k, Object alt) throws Exception {
		return graphly.getProperty(graph, vid, k, alt);
	}

	public TLongObjectHashMap<Object> collect(String k, int top, long[] vids)
			throws Exception {
		return graphly.collect(graph, k, top, vids);
	}

	public void set(String to, TLongObjectHashMap<Object> map) throws Exception {
		graphly.set(graph, to, map);
	}

	public int getEdgesCount(Dir dir, long vid, long[] at) throws Exception {
		return graphly.getEdgesCount(graph, dir, vid, at);
	}

	public SubGraph getSubGraph(String k) {
		return graphly.getSubGraph(graph, k);
	}

	public SubGraph getSubGraph(String k, long[] all) {
		return graphly.getSubGraph(graph, k, all);
	}

	public long[] getEdges(Dir in, long vid, long[] all) {
		return graphly.getEdges(graph, in, vid, all);
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
	public void setRPC(RPCDispatcher rpc) throws Exception {
		JobDispatcher jd = (JobDispatcher) rpc.getTarget("JD");
		this.graphly = (Graphly) jd.getGlobal("graphly");
	}

	public void setTempProperties(long[] before,
			Map<Long, Map<String, Object>> temps) throws InterruptedException {
		this.graphly.setTempProperties(graph, before, temps);

	}

	public JobDispatcher getJobClient() {
		return this.graphly.getJobClient();
	}

	public void commitUpdates(String... k) throws Exception {
		this.graphly.commitUpdates(graph, k);

	}

	public RPCDispatcher getRpc() {
		return this.graphly.getRpc();
	}

	public PregelClient getPregeClient() {
		return this.graphly.getPregeClient();
	}

	public ConsistentHashing getHash() {
		return this.graphly.getHash();
	}
}
