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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.server.Coordinator;
import edu.jlime.graphly.server.GraphlyCoordinatorBroadcast;
import edu.jlime.graphly.server.GraphlyCoordinatorFactory;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.storenode.rpc.AdjacencyData;
import edu.jlime.graphly.storenode.rpc.StoreNode;
import edu.jlime.graphly.storenode.rpc.StoreNodeBroadcast;
import edu.jlime.graphly.storenode.rpc.StoreNodeFactory;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.util.Gather;
import edu.jlime.jd.Dispatcher;
import edu.jlime.pregel.client.Pregel;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.util.ByteBuffer;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class Graphly implements Closeable {

	private static final int MAX_VERTEX_ITERATOR = 1000;

	public static final int NUM_JOBS = 1;

	private RPC rpc;

	Client<StoreNode, StoreNodeBroadcast> mgr;

	private ConsistentHashing consistenthash;

	private Dispatcher jobCli;

	private Map<String, SubGraph> subgraphs = new HashMap<>();

	private Logger log = Logger.getLogger(Graphly.class);

	private Pregel pregel_client;

	private long[] EMPTY_LONG_ARRAY = new long[] {};

	private Graphly(Coordinator coord, Pregel pregel_client,
			Client<StoreNode, StoreNodeBroadcast> mgr, Dispatcher jd)
					throws Exception {
		this.pregel_client = pregel_client;
		this.mgr = mgr;
		this.rpc = mgr.getRpc();
		this.jobCli = jd;
		this.consistenthash = coord.getHash();
	}

	public Pregel getPregeClient() {
		return pregel_client;
	}

	private StoreNode getClientFor(final long vertex) {
		return consistenthash.getStore(vertex);
		// Peer node = consistenthash.getNode(vertex);
		// GraphlyStoreNodeI graphlyStoreNodeI = mgr.get(node);
		// return graphlyStoreNodeI;
	}

	public Graph getGraph(String graphName) {
		return new Graph(this, graphName);
	}

	public Traversal v(String graph, long... id) {
		return new Traversal(id, getGraph(graph));
	}

	public Vertex addVertex(String graph, long id, String label)
			throws Exception {
		getClientFor(id).addVertex(graph, id, label);
		return getVertex(graph, id);
	}

	public Vertex getVertex(String graph, long id) {
		return new Vertex(id, getGraph(graph));
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

	public Edge addEdge(String graph, long id, long id2, String label,
			Object... keyValues) throws Exception {
		long where = id;
		long other = id2;
		if (id > id2) {
			where = id2;
			other = id;
		}

		getClientFor(where).addEdge(graph, id, id2, label, keyValues);
		getClientFor(other).addInEdgePlaceholder(graph, id2, id, label);
		return new Edge(id, id2, getGraph(graph));
	}

	public static Graphly build(int min) throws Exception {
		Map<String, String> d = new HashMap<>();
		d.put("app", "graphly-client," + Dispatcher.CLIENT);

		RPC rpc = new JLiMEFactory(d, null).build();

		rpc.start();

		Dispatcher jd = Dispatcher.build(min, rpc);

		jd.start();

		Pregel pregel_client = new Pregel(rpc, min);

		jd.setGlobal("pregel", pregel_client);

		Graphly build = build(rpc, pregel_client, jd, min);

		jd.setGlobal("graphly", build);

		return build;

	}

	public static Graphly build(RPC rpc, Pregel pregel_client, Dispatcher jd,
			int min) throws Exception {

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

		Client<StoreNode, StoreNodeBroadcast> mgr = rpc.manage(
				new StoreNodeFactory(rpc, "graphly"),
				new DataFilter("app", "graphly-server", true),
				rpc.getCluster().getLocalPeer());

		Client<Coordinator, GraphlyCoordinatorBroadcast> coordMgr = rpc.manage(
				new GraphlyCoordinatorFactory(rpc, "Coordinator"),
				new DataFilter("app", "coordinator", true),
				rpc.getCluster().getLocalPeer());

		mgr.waitForClient(min);
		coordMgr.waitFirst();
		return new Graphly(coordMgr.getFirst(), pregel_client, mgr, jd);
	}

	public Dispatcher getJobClient() {
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

		Map<StoreNode, TLongArrayList> map = new HashMap<>();
		for (long l : vids) {
			StoreNode node = getClientFor(l);
			TLongArrayList currList = map.get(node);
			if (currList == null) {
				currList = new TLongArrayList();
				map.put(node, currList);
			}
			currList.add(l);
		}

		if (map.size() == 1) {
			StoreNode node = map.entrySet().iterator().next().getKey();
			return node.getEdges(graph, dir, max_edges, vids);
		}

		final TLongHashSet ret = new TLongHashSet();

		List<Future<Void>> futs = new ArrayList<>();

		for (final Entry<StoreNode, TLongArrayList> e : map.entrySet()) {
			futs.add(svc.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					long[] edges = e.getKey().getEdges(graph, dir, max_edges,
							e.getValue().toArray());
					synchronized (ret) {
						ret.addAll(edges);
					}
					return null;
				}
			}));

		}
		svc.shutdown();
		for (Future<Void> future : futs) {
			future.get();
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

	public Count countEdges(final String graph, final Dir dir,
			final int max_edges, long[] keys, float[] values,
			final long[] toFilter) throws Exception {
		ExecutorService svc = Executors.newCachedThreadPool();

		log.info("Hashing keys to count edges");

		Map<StoreNode, TLongArrayList> map = hashKeys(keys);

		if (map.size() == 1) {
			StoreNode node = map.entrySet().iterator().next().getKey();
			return node.countEdges(graph, dir, max_edges, keys, values,
					toFilter);
		}

		final TLongFloatMap ret = new TLongFloatHashMap();

		log.info("Sending count edge requests.");
		final TLongFloatMap data = new TLongFloatHashMap(keys, values);

		for (final Entry<StoreNode, TLongArrayList> e : map.entrySet()) {
			svc.execute(new Runnable() {
				@Override
				public void run() {
					try {
						TLongFloatMap prevCounts = new TLongFloatHashMap();
						TLongIterator itV = e.getValue().iterator();
						while (itV.hasNext()) {
							long v = itV.next();
							prevCounts.put(v, data.get(v));
						}
						Count count = e.getKey().countEdges(graph, dir,
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

		return new Count(ret.keys(), ret.values());
	}

	public Map<StoreNode, TLongArrayList> hashKeys(long[] data) {
		Map<Peer, TLongArrayList> map = consistenthash.hashKeys(data);
		Map<StoreNode, TLongArrayList> ret = new HashMap<>();
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
			Map<StoreNode, TLongArrayList> divided = hashKeys(vids);
			for (Entry<StoreNode, TLongArrayList> l : divided.entrySet()) {
				TLongObjectMap<Object> properties = l.getKey()
						.getProperties(graph, k, top, l.getValue());

				ret.putAll(properties);
			}
			return ret;
		}

		TLongObjectHashMap<Object> ret = new TLongObjectHashMap<Object>();

		TreeMultimap<Comparable, Long> sorted = TreeMultimap.create();

		Map<StoreNode, TLongArrayList> divided = hashKeys(vids);
		for (Entry<StoreNode, TLongArrayList> l : divided.entrySet()) {
			TLongObjectMap<Object> properties = l.getKey().getProperties(graph,
					k, top, l.getValue());
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

	public void set(final String graph, final String to,
			final TLongObjectMap<Object> map) throws Exception {
		Map<StoreNode, TLongArrayList> divided = hashKeys(map.keys());

		ExecutorService exec = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		for (final Entry<StoreNode, TLongArrayList> e : divided.entrySet()) {
			exec.execute(new Runnable() {

				@Override
				public void run() {
					TLongObjectHashMap<Object> submap = new TLongObjectHashMap<>();
					TLongIterator it = e.getValue().iterator();
					while (it.hasNext()) {
						long v = it.next();
						submap.put(v, map.get(v));
					}
					try {
						e.getKey().setProperties(graph, to, submap);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
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
		Map<StoreNode, TLongArrayList> map = hashKeys(before);
		// ExecutorService svc = Executors.newCachedThreadPool();
		for (Entry<StoreNode, TLongArrayList> entry : map.entrySet()) {
			final StoreNode node = entry.getKey();
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
		Map<StoreNode, TLongArrayList> div = hashKeys(array);
		for (Entry<StoreNode, TLongArrayList> entry : div.entrySet()) {
			props.putAll(entry.getKey().getProperties(graph, array, k));
		}
		return props;
	}

	public RPC getRpc() {
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
		for (StoreNode gsn : mgr.getAll()) {
			gsn.setDefault(graph, k, v);
		}
	}

	public Object getDefaultValue(String graph, String k) throws Exception {
		StoreNode graphlyStoreNodeI = mgr
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
		for (StoreNode gsn : mgr.getAll()) {
			gsn.setDefaultDouble(graph, k, v);
		}
	}

	public void setDouble(String graph, long v, String k, double currentVal)
			throws Exception {
		getClientFor(v).setDouble(graph, v, k, currentVal);
	}

	public Set<String> listGraphs() throws Exception {
		HashSet<String> ret = new HashSet<>();
		for (StoreNode gsn : mgr.getAll()) {
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
		for (StoreNode gsn : mgr.getAll()) {
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

	public Traversal v(String graph, TLongHashSet ids) {
		return new Traversal(ids, getGraph(graph));
	}

	public void setTempFloats(final String graph, final String k,
			final boolean add, final TLongFloatMap v) throws Exception {

		Map<StoreNode, TLongArrayList> map = hashKeys(v.keys());

		if (map.size() == 1) {
			map.keySet().iterator().next().setTempFloats(graph, k, add,
					v.keys(), v.values());
		} else {

			ExecutorService exec = Executors.newFixedThreadPool(
					Runtime.getRuntime().availableProcessors());

			List<Future<Void>> futures = new ArrayList<>();

			for (final Entry<StoreNode, TLongArrayList> entry : map
					.entrySet()) {
				futures.add(exec.submit(new Callable<Void>() {

					@Override
					public Void call() throws Exception {

						final StoreNode node = entry.getKey();
						final long[] current = entry.getValue().toArray();
						TLongFloatMap subProp = new TLongFloatHashMap();
						for (long l : current) {
							subProp.put(l, v.get(l));
						}
						System.out.println("Sending to " + node + " "
								+ subProp.size() + " pairs.");
						node.setTempFloats(graph, k, add, subProp.keys(),
								subProp.values());
						return null;
					}
				}));

			}

			exec.shutdown();
			for (Future<Void> future : futures) {
				future.get();
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

	public void setFloat(final String graph, final String k,
			final TLongFloatMap auth2) throws Exception {
		Map<StoreNode, TLongArrayList> map = hashKeys(auth2.keys());

		ExecutorService exec = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<Future<Void>> futures = new ArrayList<>();

		for (final Entry<StoreNode, TLongArrayList> entry : map.entrySet()) {

			futures.add(exec.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					final StoreNode node = entry.getKey();
					final long[] current = entry.getValue().toArray();
					TLongFloatMap subProp = new TLongFloatHashMap();
					for (long l : current) {
						subProp.put(l, auth2.get(l));
					}
					node.setFloats(graph, k, subProp);
					return null;
				}
			}));

		}
		exec.shutdown();
		for (Future<Void> future : futures) {
			future.get();
		}
	}

	public void setProperty(String graph, TLongHashSet vertices, String k,
			String val) {
		Map<StoreNode, TLongArrayList> map = hashKeys(vertices.toArray());
		for (Entry<StoreNode, TLongArrayList> entry : map.entrySet()) {
			final StoreNode node = entry.getKey();

			try {
				node.setProperty(graph, k, val, entry.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Map<String, TLongObjectMap<Object>> getAllProperties(
			final String graph, final long[] array) throws Exception {
		Map<String, TLongObjectMap<Object>> props = new HashMap<>();
		Map<StoreNode, TLongArrayList> div = hashKeys(array);

		ExecutorService exec = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		ArrayList<Future<Map<String, PropertyData>>> futs = new ArrayList<>();

		for (final Entry<StoreNode, TLongArrayList> entry : div.entrySet()) {
			Future<Map<String, PropertyData>> res = exec
					.submit(new Callable<Map<String, PropertyData>>() {

						@Override
						public Map<String, PropertyData> call()
								throws Exception {
							Map<String, PropertyData> remote = entry.getKey()
									.getAllProperties(graph, array);

							return remote;
						}
					});
			futs.add(res);

		}
		exec.shutdown();

		for (Future<Map<String, PropertyData>> future : futs) {
			for (Entry<String, PropertyData> e : future.get().entrySet()) {
				TLongObjectMap<Object> sm = props.get(e.getKey());
				PropertyData data = e.getValue();

				if (sm == null) {
					sm = new TLongObjectHashMap<>();
					props.put(e.getKey(), sm);
				}
				for (int i = 0; i < data.keys.length; i++) {
					sm.put(data.keys[i], data.values[i]);
				}
			}
		}

		return props;
	}

	public Map<String, TLongFloatMap> getAllFloatProperties(final String graph,
			final long[] array) throws Exception {
		Map<String, TLongFloatMap> props = new HashMap<>();
		Map<StoreNode, TLongArrayList> div = hashKeys(array);

		ExecutorService exec = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		ArrayList<Future<Map<String, TLongFloatMap>>> futs = new ArrayList<Future<Map<String, TLongFloatMap>>>();

		for (final Entry<StoreNode, TLongArrayList> entry : div.entrySet()) {
			Future<Map<String, TLongFloatMap>> res = exec
					.submit(new Callable<Map<String, TLongFloatMap>>() {

						@Override
						public Map<String, TLongFloatMap> call()
								throws Exception {
							Map<String, TLongFloatMap> remote = entry.getKey()
									.getAllFloatProperties(graph, array);
							return remote;
						}
					});
			futs.add(res);
		}

		exec.shutdown();

		for (Future<Map<String, TLongFloatMap>> future : futs) {
			for (Entry<String, TLongFloatMap> e : future.get().entrySet()) {
				TLongFloatMap sm = props.get(e.getKey());
				if (sm == null) {
					sm = e.getValue();
					props.put(e.getKey(), sm);
				} else
					sm.putAll(e.getValue());
			}
		}

		return props;
	}

	public TLongObjectMap<long[]> getAllEdges(final String graph, long[] array,
			final Dir dir) throws Exception {
		TLongObjectMap<long[]> ret = new TLongObjectHashMap<long[]>();

		Map<StoreNode, TLongArrayList> div = hashKeys(array);

		ExecutorService exec = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		ArrayList<Future<AdjacencyData>> futs = new ArrayList<>();

		for (final Entry<StoreNode, TLongArrayList> entry : div.entrySet()) {
			Future<AdjacencyData> res = exec
					.submit(new Callable<AdjacencyData>() {
						@Override
						public AdjacencyData call() throws Exception {
							return entry.getKey().getAllEdges(graph,
									entry.getValue(), dir);
						}
					});
			futs.add(res);
		}
		exec.shutdown();
		for (Future<AdjacencyData> future : futs) {
			AdjacencyData data = future.get();
			for (int i = 0; i < data.keys.length; i++) {
				ret.put(data.keys[i], data.values[i]);
			}

		}

		return ret;
	}

	public StoreNode getLocalStore() {
		return mgr.get(mgr.getLocalPeer());
	}

	public void createSubgraph(String from, String sg, long[] vids)
			throws Exception {
		mgr.broadcast().createSubgraph(from, sg, vids);
	}
}
