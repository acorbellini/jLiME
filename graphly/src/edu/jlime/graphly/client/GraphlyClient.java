package edu.jlime.graphly.client;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.server.GraphlyCoordinator;
import edu.jlime.graphly.server.GraphlyCoordinatorBroadcast;
import edu.jlime.graphly.server.GraphlyCoordinatorFactory;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeI;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeIBroadcast;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeIFactory;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.util.Gather;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.pregel.client.PregelClient;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.util.ByteBuffer;
import gnu.trove.impl.hash.TLongFloatHash;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyClient implements Closeable {

	private static final int MAX_VERTEX_ITERATOR = 1000;

	public static final int NUM_JOBS = 1;

	private RPCDispatcher rpc;

	ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr;

	private ConsistentHashing consistenthash;

	private JobDispatcher jobCli;

	private Map<String, SubGraph> subgraphs = new HashMap<>();

	private Logger log = Logger.getLogger(GraphlyClient.class);

	private PregelClient pregel_client;

	private long[] EMPTY_LONG_ARRAY = new long[] {};

	private GraphlyClient(GraphlyCoordinator coord, PregelClient pregel_client,
			ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr,
			JobDispatcher jd) throws Exception {
		this.pregel_client = pregel_client;
		this.mgr = mgr;
		this.rpc = mgr.getRpc();
		this.jobCli = jd;
		this.consistenthash = coord.getHash();
	}

	public PregelClient getPregeClient() {
		return pregel_client;
	}

	private GraphlyStoreNodeI getClientFor(final long vertex) {
		return consistenthash.getStore(vertex);
		// Peer node = consistenthash.getNode(vertex);
		// GraphlyStoreNodeI graphlyStoreNodeI = mgr.get(node);
		// return graphlyStoreNodeI;
	}

	public GraphlyGraph getGraph(String graphName) {
		return new GraphlyGraph(this, graphName);
	}

	public GraphlyTraversal v(String graph, long... id) {
		return new GraphlyTraversal(id, getGraph(graph));
	}

	public GraphlyVertex addVertex(String graph, long id, String label)
			throws Exception {
		getClientFor(id).addVertex(graph, id, label);
		return getVertex(graph, id);
	}

	public GraphlyVertex getVertex(String graph, long id) {
		return new GraphlyVertex(id, getGraph(graph));
	}

	public void close() {
		try {
			this.rpc.stop();
			this.jobCli.stop();
			this.pregel_client.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getLabel(String graph, long id) throws Exception {
		return getClientFor(id).getLabel(graph, id);
	}

	public void remove(String graph, long id) throws Exception {
		getClientFor(id).removeVertex(graph, id);
	}

	public GraphlyEdge addEdge(String graph, long id, long id2, String label,
			Object... keyValues) throws Exception {
		long where = id;
		long other = id2;
		if (id > id2) {
			where = id2;
			other = id;
		}

		getClientFor(where).addEdge(graph, id, id2, label, keyValues);
		getClientFor(other).addInEdgePlaceholder(graph, id2, id, label);
		return new GraphlyEdge(id, id2, getGraph(graph));
	}

	public static GraphlyClient build(int min) throws Exception {
		Map<String, String> d = new HashMap<>();
		d.put("app", "graphly-client," + JobDispatcher.CLIENT);

		RPCDispatcher rpc = new JLiMEFactory(d, null).build();

		rpc.start();

		JobDispatcher jd = JobDispatcher.build(min, rpc);

		jd.start();

		PregelClient pregel_client = new PregelClient(rpc, min);

		jd.setGlobal("pregel", pregel_client);

		GraphlyClient build = build(rpc, pregel_client, jd, min);

		jd.setGlobal("graphly", build);

		return build;

	}

	public static GraphlyClient build(RPCDispatcher rpc,
			PregelClient pregel_client, JobDispatcher jd, int min)
					throws Exception {

		TypeConverters tc = rpc.getMarshaller().getTc();
		tc.registerTypeConverter(Dir.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buffer, Peer cliID)
					throws Exception {
				Dir dir = (Dir) o;
				buffer.put(dir.getID());
			}

			@Override
			public Object fromArray(ByteBuffer buff) throws Exception {
				return Dir.fromID(buff.get());
			}
		});

		ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr = rpc
				.manage(new GraphlyStoreNodeIFactory(rpc, "graphly"),
						new DataFilter("app", "graphly-server", true),
						rpc.getCluster().getLocalPeer());

		ClientManager<GraphlyCoordinator, GraphlyCoordinatorBroadcast> coordMgr = rpc
				.manage(new GraphlyCoordinatorFactory(rpc, "Coordinator"),
						new DataFilter("app", "coordinator", true),
						rpc.getCluster().getLocalPeer());

		mgr.waitForClient(min);
		coordMgr.waitFirst();
		return new GraphlyClient(coordMgr.getFirst(), pregel_client, mgr, jd);
	}

	public JobDispatcher getJobClient() {
		return jobCli;
	}

	public long[] getEdges(String graph, Dir dir, long... vids)
			throws Exception {
		return getEdgesMax(graph, dir, -1, vids);
	}

	public long[] getEdgesMax(final String graph, final Dir dir,
			final int max_edges, long... vids) throws Exception {

		if (vids.length == 1) {
			return getClientFor(vids[0]).getEdges(graph, dir, max_edges, vids);
		}

		ExecutorService svc = Executors.newCachedThreadPool();

		Map<GraphlyStoreNodeI, TLongArrayList> map = new HashMap<>();
		for (long l : vids) {
			GraphlyStoreNodeI node = getClientFor(l);
			TLongArrayList currList = map.get(node);
			if (currList == null) {
				currList = new TLongArrayList();
				map.put(node, currList);
			}
			currList.add(l);
		}

		if (map.size() == 1) {
			GraphlyStoreNodeI node = map.entrySet().iterator().next().getKey();
			try {
				return node.getEdges(graph, dir, max_edges, vids);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

		final TLongHashSet ret = new TLongHashSet();
		for (final Entry<GraphlyStoreNodeI, TLongArrayList> e : map
				.entrySet()) {
			svc.execute(new Runnable() {

				@Override
				public void run() {
					try {
						long[] edges = e.getKey().getEdges(graph, dir,
								max_edges, e.getValue().toArray());
						synchronized (ret) {
							ret.addAll(edges);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		svc.shutdown();
		try {
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		return ret.toArray();
	}

	public void addEdges(String graph, long vid, Dir dir, long[] dests)
			throws Exception {
		if (dir.equals(Dir.BOTH))
			throw new IllegalArgumentException(
					"Can't load bidirectional edges.");
		getClientFor(vid).addEdges(graph, vid, dir, dests);
	}

	public ConsistentHashing getHash() {
		return consistenthash;
	}

	public GraphlyCount countEdges(final String graph, final Dir dir,
			final int max_edges, long[] keys, float[] values,
			final long[] toFilter) throws Exception {
		ExecutorService svc = Executors.newCachedThreadPool();

		log.info("Hashing keys to count edges");

		Map<GraphlyStoreNodeI, TLongArrayList> map = hashKeys(keys);

		if (map.size() == 1) {
			GraphlyStoreNodeI node = map.entrySet().iterator().next().getKey();
			return node.countEdges(graph, dir, max_edges, keys, values,
					toFilter);
		}

		final TLongFloatHashMap ret = new TLongFloatHashMap();

		log.info("Sending count edge requests.");
		final TLongFloatHashMap data = new TLongFloatHashMap(keys, values);

		for (final Entry<GraphlyStoreNodeI, TLongArrayList> e : map
				.entrySet()) {
			svc.execute(new Runnable() {
				@Override
				public void run() {
					try {
						TLongFloatHashMap prevCounts = new TLongFloatHashMap();
						TLongIterator itV = e.getValue().iterator();
						while (itV.hasNext()) {
							long v = itV.next();
							prevCounts.put(v, data.get(v));
						}
						GraphlyCount count = e.getKey().countEdges(graph, dir,
								max_edges, prevCounts.keys(),
								prevCounts.values(), toFilter);
						synchronized (ret) {
							TLongFloatIterator it = count.iterator();
							while (it.hasNext()) {
								it.advance();
								ret.adjustOrPutValue(it.key(), it.value(),
										it.value());
							}

						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		svc.shutdown();
		try {
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		return new GraphlyCount(ret.keys(), ret.values());
	}

	public Map<GraphlyStoreNodeI, TLongArrayList> hashKeys(long[] data) {
		Map<Peer, TLongArrayList> map = consistenthash.hashKeys(data);
		Map<GraphlyStoreNodeI, TLongArrayList> ret = new HashMap<>();
		for (Entry<Peer, TLongArrayList> e : map.entrySet()) {
			ret.put(mgr.get(e.getKey()), e.getValue());
		}
		return ret;
	}

	public long getRandomEdge(String graph, long before, long[] subset, Dir d)
			throws Exception {
		return getClientFor(before).getRandomEdge(graph, before, subset, d);
	}

	public Object getProperty(String graph, String string, long vid)
			throws Exception {
		return getClientFor(vid).getProperty(graph, vid, string);
	}

	public void setProperties(String graph, long vid, Map<String, Object> value)
			throws Exception {
		for (Entry<String, Object> m : value.entrySet()) {
			setProperty(graph, vid, m.getKey(), m.getValue());
		}
	}

	public void setProperty(String graph, long vid, String k, Object v)
			throws Exception {
		getClientFor(vid).setProperty(graph, vid, k, v);
	}

	public Object getProperty(String graph, long vid, String k, Object alt)
			throws Exception {
		Object prop = getProperty(graph, k, vid);
		if (prop == null)
			return alt;
		return prop;
	}

	public TLongObjectHashMap<Object> collect(String graph, String k, int top,
			long[] vids) throws Exception {
		if (top <= 0) {
			TLongObjectHashMap<Object> ret = new TLongObjectHashMap<Object>();
			Map<GraphlyStoreNodeI, TLongArrayList> divided = hashKeys(vids);
			for (Entry<GraphlyStoreNodeI, TLongArrayList> l : divided
					.entrySet()) {
				TLongObjectHashMap<Object> properties = l.getKey()
						.getProperties(graph, k, top, l.getValue());

				ret.putAll(properties);
			}
			return ret;
		}

		TLongObjectHashMap<Object> ret = new TLongObjectHashMap<Object>();

		TreeMultimap<Comparable, Long> sorted = TreeMultimap.create();

		Map<GraphlyStoreNodeI, TLongArrayList> divided = hashKeys(vids);
		for (Entry<GraphlyStoreNodeI, TLongArrayList> l : divided.entrySet()) {
			TLongObjectHashMap<Object> properties = l.getKey()
					.getProperties(graph, k, top, l.getValue());
			TLongObjectIterator<Object> it = properties.iterator();
			while (it.hasNext()) {
				it.advance();
				Comparable value = (Comparable) it.value();
				if (value != null)
					if (sorted.size() < top) {
						sorted.put(value, it.key());
					} else {
						Comparable toRemove = sorted.asMap().firstKey();
						if (toRemove.compareTo(value) < 0) {
							NavigableSet<Long> navigableSet = sorted
									.get(toRemove);
							Long f = navigableSet.first();
							navigableSet.remove(f);
							sorted.put(value, it.key());
						}
					}
			}

			// ret.putAll(properties);
		}
		for (Entry<Comparable, Long> l : sorted.entries()) {
			ret.put(l.getValue(), l.getKey());
		}

		return ret;
	}

	public void set(String graph, String to, TLongObjectHashMap<Object> map)
			throws Exception {
		Map<GraphlyStoreNodeI, TLongArrayList> divided = hashKeys(map.keys());
		for (Entry<GraphlyStoreNodeI, TLongArrayList> e : divided.entrySet()) {
			TLongObjectHashMap<Object> submap = new TLongObjectHashMap<>();
			TLongObjectIterator<Object> it = map.iterator();
			while (it.hasNext()) {
				it.advance();
				if (e.getValue().contains(it.key()))
					submap.put(it.key(), it.value());
			}
			e.getKey().setProperties(graph, to, submap);
		}
	}

	public int getEdgesCount(String graph, Dir dir, long vid, TLongHashSet at)
			throws Exception {
		return getClientFor(vid).getEdgeCount(graph, vid, dir, at);
	}

	public SubGraph getSubGraph(String graph, String string, long[] all) {
		String id = graph + "." + string;
		SubGraph sg = subgraphs.get(id);
		if (sg == null && all != null) {
			synchronized (subgraphs) {
				if (sg == null) {
					sg = new SubGraph(getGraph(graph), all);
					subgraphs.put(id, sg);
				}
			}
		}
		return sg;
	}

	public void commitUpdates(String graph, String... k) throws Exception {
		mgr.broadcast().commitUpdates(graph, k);
	}

	public void setTempProperties(String graph, long[] before,
			final Map<Long, Map<String, Object>> temps)
					throws InterruptedException {
		Map<GraphlyStoreNodeI, TLongArrayList> map = hashKeys(before);
		// ExecutorService svc = Executors.newCachedThreadPool();
		for (Entry<GraphlyStoreNodeI, TLongArrayList> entry : map.entrySet()) {
			final GraphlyStoreNodeI node = entry.getKey();
			final long[] current = entry.getValue().toArray();

			// svc.execute(new Runnable() {
			// @Override
			// public void run() {
			HashMap<Long, Map<String, Object>> subProp = new HashMap<>();
			for (long l : current) {
				subProp.put(l, temps.get(l));
			}
			try {
				node.setTempProperties(graph, subProp);
			} catch (Exception e) {
				e.printStackTrace();
			}
			// }
			// });
		}
		// svc.shutdown();
		// svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	public SubGraph getSubGraph(String graph, String string) {
		return getSubGraph(graph, string, null);
	}

	public long[] getEdgesFiltered(String graph, Dir in, long vid,
			TLongHashSet all) {
		try {
			long[] edges = getEdges(graph, in, vid);

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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return EMPTY_LONG_ARRAY;
	}

	public Map<Long, Map<String, Object>> getProperties(String graph,
			long[] array, String... k) throws Exception {
		Map<Long, Map<String, Object>> props = new HashMap<>();
		Map<GraphlyStoreNodeI, TLongArrayList> div = hashKeys(array);
		for (Entry<GraphlyStoreNodeI, TLongArrayList> entry : div.entrySet()) {
			props.putAll(entry.getKey().getProperties(graph, array, k));
		}
		return props;
	}

	public RPCDispatcher getRpc() {
		return rpc;
	}

	public int getVertexCount(String graph) throws Exception {

		int count = 0;
		Map<Peer, Integer> res = mgr.broadcast().getVertexCount(graph);
		for (Entry<Peer, Integer> e : res.entrySet()) {
			count += e.getValue();
		}
		return count;

	}

	public VertexList vertices(String graph) throws Exception {
		return new VertexList(graph, this, MAX_VERTEX_ITERATOR);
	}

	public void setDefaultValue(String graph, String k, Object v)
			throws Exception {
		for (GraphlyStoreNodeI gsn : mgr.getAll()) {
			gsn.setDefault(graph, k, v);
		}
	}

	public Object getDefaultValue(String graph, String k) throws Exception {
		GraphlyStoreNodeI graphlyStoreNodeI = mgr
				.get(mgr.getRpc().getCluster().getLocalPeer());
		if (graphlyStoreNodeI != null) {
			return graphlyStoreNodeI.getDefault(graph, k);
		}
		return mgr.getFirst().getDefault(graph, k);
	}

	public double getDouble(String graph, long v, String k) throws Exception {
		return getClientFor(v).getDouble(graph, v, k);
	}

	public void setDefaultDouble(String graph, String k, double v)
			throws Exception {
		for (GraphlyStoreNodeI gsn : mgr.getAll()) {
			gsn.setDefaultDouble(graph, k, v);
		}
	}

	public void setDouble(String graph, long v, String k, double currentVal)
			throws Exception {
		getClientFor(v).setDouble(graph, v, k, currentVal);
	}

	public Set<String> listGraphs() throws Exception {
		HashSet<String> ret = new HashSet<>();
		for (GraphlyStoreNodeI gsn : mgr.getAll()) {
			ret.addAll(gsn.getGraphs());
		}
		return ret;
	}

	public float getFloat(String graph, long v, String k) throws Exception {
		return getClientFor(v).getFloat(graph, v, k);
	}

	public void setFloat(String graph, long v, String k, float currentVal)
			throws Exception {
		getClientFor(v).setFloat(graph, v, k, currentVal);
	}

	public void setDefaultFloat(String graph, String k, float v)
			throws Exception {
		for (GraphlyStoreNodeI gsn : mgr.getAll()) {
			gsn.setDefaultFloat(graph, k, v);
		}
	}

	public <T> List<T> gather(String graph, Gather<T> g) throws Exception {
		ArrayList<T> ret = new ArrayList<>();
		Map<Peer, Object> map = mgr.broadcast().gather(graph, g);
		for (Entry<Peer, Object> t : map.entrySet()) {
			ret.add((T) t.getValue());
		}
		return ret;

	}

	public float getDefaultFloat(String graph, String prop) throws Exception {
		return mgr.getFirst().getDefaultFloat(graph, prop);
	}

	public GraphlyTraversal v(String graph, TLongHashSet ids) {
		return new GraphlyTraversal(ids, getGraph(graph));
	}

	public void setTempFloats(final String graph, final String k,
			final boolean add, final TLongFloatHashMap v) throws Exception {

		Map<GraphlyStoreNodeI, TLongArrayList> map = hashKeys(v.keys());

		if (map.size() == 1) {
			map.keySet().iterator().next().setTempFloats(graph, k, add, v);
		} else {

			ExecutorService exec = Executors.newFixedThreadPool(
					Runtime.getRuntime().availableProcessors());

			for (final Entry<GraphlyStoreNodeI, TLongArrayList> entry : map
					.entrySet()) {
				exec.execute(new Runnable() {

					@Override
					public void run() {

						final GraphlyStoreNodeI node = entry.getKey();
						final long[] current = entry.getValue().toArray();
						TLongFloatHashMap subProp = new TLongFloatHashMap();
						for (long l : current) {
							subProp.put(l, v.get(l));
						}
						try {
							node.setTempFloats(graph, k, add, subProp);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});

			}

			exec.shutdown();
			try {
				exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public void commitFloatUpdates(String graph, String... props)
			throws Exception {
		mgr.broadcast().commitFloatUpdates(graph, props);

	}

	public void updateFloatProperty(String graph, String prop,
			DivideUpdateProperty upd) throws Exception {
		mgr.broadcast().updateFloatProperty(graph, prop, upd);

	}

	public float getFloat(String graph, long v, String k, float alt)
			throws Exception {
		return getClientFor(v).getFloat(graph, v, k, alt);

	}

	public void setFloat(String graph, String k, TLongFloatHashMap auth2) {
		Map<GraphlyStoreNodeI, TLongArrayList> map = hashKeys(auth2.keys());
		for (Entry<GraphlyStoreNodeI, TLongArrayList> entry : map.entrySet()) {
			final GraphlyStoreNodeI node = entry.getKey();
			final long[] current = entry.getValue().toArray();
			TLongFloatHashMap subProp = new TLongFloatHashMap();
			for (long l : current) {
				subProp.put(l, auth2.get(l));
			}
			try {
				node.setFloats(graph, k, subProp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void setProperty(String graph, TLongHashSet vertices, String k,
			String val) {
		Map<GraphlyStoreNodeI, TLongArrayList> map = hashKeys(
				vertices.toArray());
		for (Entry<GraphlyStoreNodeI, TLongArrayList> entry : map.entrySet()) {
			final GraphlyStoreNodeI node = entry.getKey();

			try {
				node.setProperty(graph, k, val, entry.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
