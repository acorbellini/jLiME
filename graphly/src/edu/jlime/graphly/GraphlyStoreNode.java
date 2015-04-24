package edu.jlime.graphly;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.TreeMultimap;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.store.LocalStore;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.Gather;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyStoreNode implements GraphlyStoreNodeI {

	private static byte VERTEX = 0x0;
	private static byte ADJACENCY = 0x1;
	private static byte VERTEX_PROP = 0x2;
	private static byte EDGE_PROP = 0x3;
	private static byte GRAPH = 0x3;

	Logger log = Logger.getLogger(GraphlyStoreNode.class);

	// private Graph graph;
	private LocalStore store;

	Cache<String, Boolean> graph_cache = CacheBuilder.newBuilder()
			.maximumSize(100).build();

	Cache<Long, long[]> adj_cache = CacheBuilder.newBuilder().maximumSize(1000)
			.build();

	Cache<Long, Boolean> vertex_cache = CacheBuilder.newBuilder()
			.maximumSize(1000).build();

	Cache<String, Integer> size_cache = CacheBuilder.newBuilder()
			.maximumSize(1000).build();

	private File localRanges;
	private List<Integer> ranges = new ArrayList<>();
	private InMemoryGraphProperties props = new InMemoryGraphProperties();
	private InMemoryGraphDoubleProperties doubleProps = new InMemoryGraphDoubleProperties();
	private InMemoryGraphFloatProperties floatProps = new InMemoryGraphFloatProperties();

	private Map<String, Map<Long, Map<String, Object>>> temps = new ConcurrentHashMap<>();

	// Store store;

	public GraphlyStoreNode(String localpath, RPCDispatcher rpc)
			throws IOException {

		try {
			Files.createDirectory(Paths.get(localpath));
		} catch (Exception e) {
		}

		this.localRanges = new File(localpath + "/ranges.prop");
		if (!localRanges.exists())
			localRanges.createNewFile();

		Properties prop = new Properties();
		prop.load(new FileReader(localRanges));
		String rangeString = prop.getProperty("ranges");
		if (rangeString != null && !rangeString.isEmpty()) {
			rangeString = rangeString.replaceAll("\\[", "")
					.replaceAll("\\s", "").replaceAll("\\]", "");
			for (String rangeVal : rangeString.split(",")) {
				ranges.add(Integer.valueOf(rangeVal));
			}
		}
		// this.graph = Neo4jGraph.open(localpath + "/neo4j");
		this.store = new LocalStore(localpath);
	}

	@Override
	public List<Integer> getRanges() {
		log.info("Obtaning Ranges (size " + ranges.size() + ")");
		return ranges;
	}

	@Override
	public synchronized void addRange(int range) throws IOException {
		this.ranges.add(range);
		FileWriter writer = new FileWriter(localRanges);
		Properties prop = new Properties();
		prop.setProperty("ranges", this.ranges.toString());
		prop.store(writer, "");
		writer.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#putEdges(edu.jlime.collections.
	 * adjacencygraph.get.GetType, java.lang.Long, long[])
	 */
	@Override
	public void addEdges(String graph, long k, Dir type, long[] list)
			throws Exception {
		long id = k;

		if (type.equals(Dir.IN))
			id = -id - 1;

		store.store(buildAdjacencyKey(graph, id),
				DataTypeUtils.longArrayToByteArray(list));

		addVertex(graph, k, "");

		addGraph(graph);
		// for (long l : list) {
		// addVertex(l, "");
		// }
	}

	private void addGraph(final String graph2) throws ExecutionException {
		graph_cache.get(graph2, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				byte[] id = buildGraphKey(graph2);
				byte[] res = store.load(id);
				if (res == null)
					store.store(id, graph2.getBytes());
				return true;
			}
		});
	}

	private static byte[] buildGraphKey(String graph) {
		ByteBuffer buff = new ByteBuffer(1 + 8);
		buff.put(GRAPH);
		buff.putString(graph);
		return buff.build();
	}

	private static byte[] buildAdjacencyKey(String graph, long id) {
		ByteBuffer buff = new ByteBuffer(1 + 8);
		buff.put(ADJACENCY);
		buff.putString(graph);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private static byte[] buildVertexKey(String graph, long id) {
		ByteBuffer buff = new ByteBuffer(1 + 8);
		buff.put(VERTEX);
		buff.putString(graph);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private static byte[] buildVertexPropertyKey(String graph, long id,
			String key) {
		byte[] b = key.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 8 + b.length);
		buff.put(VERTEX_PROP);
		buff.putString(graph);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		buff.putRawByteArray(b);
		return buff.build();
	}

	Random random = new Random(System.currentTimeMillis());

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdges(edu.jlime.collections.
	 * adjacencygraph.get.GetType, java.lang.Long)
	 */
	@Override
	public long[] getEdges(String graph, Dir type, int max_edges, long[] id)
			throws ExecutionException {
		if (id.length == 1) {
			return getEdges(graph, type, id[0]);
		}

		TLongArrayList ret = new TLongArrayList();
		for (long l : id) {
			long[] edges = getEdges(graph, type, l);
			if (edges.length > max_edges && max_edges > 0) {
				int in = 0;

				for (in = 0; in < edges.length && ret.size() < max_edges; in++) {
					int rn = edges.length - in;
					int rm = max_edges - ret.size();
					if (random.nextDouble() * rn < rm)
						ret.add(edges[in]);
				}
				if (log.isDebugEnabled())
					log.debug("Finished filtering for " + l);
			} else
				ret.addAll(edges);
		}
		if (log.isDebugEnabled())
			log.debug("Returning " + ret.size());
		return ret.toArray();

	}

	private long[] getEdges(String graph, Dir type, long id)
			throws ExecutionException {
		if (type.equals(Dir.BOTH)) {
			TLongHashSet list = new TLongHashSet();
			list.addAll(getEdges0(graph, id));
			list.addAll(getEdges0(graph, -id - 1));
			return list.toArray();
		}

		if (type.equals(Dir.IN))
			id = -id - 1;

		return getEdges0(graph, id);
	}

	private long[] getEdges0(final String graph, final long id)
			throws ExecutionException {
		return adj_cache.get(id, new Callable<long[]>() {
			@Override
			public long[] call() throws Exception {
				byte[] array;
				try {
					array = store.load(buildAdjacencyKey(graph, id));
					if (array != null) {
						long[] byteArrayToLongArray = DataTypeUtils
								.byteArrayToLongArray(array);
						return byteArrayToLongArray;
					}
				} catch (Exception e) {
					e.printStackTrace();

				}
				return new long[] {};
			}
		});

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#setProperty(java.lang.Long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	public void setProperty(String graph, long vid, String k, Object val)
			throws Exception {
		props.put(graph, vid, k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getProperty(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	public Object getProperty(String graph, long vid, String k)
			throws Exception {
		return props.get(graph, vid, k);
	}

	@Override
	public void addVertex(final String graph, final long id, String label)
			throws Exception {
		vertex_cache.get(id, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				byte[] vk = buildVertexKey(graph, id);
				if (store.load(vk) == null)
					store.store(vk, DataTypeUtils.longToByteArray(id));
				return true;
			}
		});
	}

	@Override
	public String getLabel(String graph, long id) throws Exception {
		return null;
	}

	@Override
	public void addEdge(String graph, long orig, long dest, String label,
			Object[] keyValues) throws Exception {
	}

	@Override
	public void removeVertex(String graph, long id) throws Exception {
	}

	@Override
	public void addInEdgePlaceholder(String graph, long id2, long id,
			String label) throws Exception {
	}

	Semaphore sem = new Semaphore(2);
	private Map<String, Object> defaults = new ConcurrentHashMap<>();
	private TObjectDoubleHashMap<String> defaultDoubleMap = new TObjectDoubleHashMap<>();
	private TObjectFloatHashMap<String> defaultFloatMap = new TObjectFloatHashMap<>();

	@Override
	public GraphlyCount countEdges(String graph, Dir dir, int max_edges,
			long[] vids) throws Exception {
		log.info("Counting edges in dir " + dir + " with max " + max_edges
				+ " and vertices " + vids.length + ".");
		TLongIntHashMap map = new TLongIntHashMap();
		// Serializing in this spot leads to amazing performance
		// Don't really know why, I guess has something to do with cache lines.
		long[][] res = new long[vids.length][];
		int cont = 0;
		synchronized (this) {
			for (long l : vids) {
				long[] curr = getEdges(graph, dir, max_edges, new long[] { l });
				res[cont++] = curr;

			}
		}
		for (long[] curr : res) {
			for (long m : curr) {
				map.adjustOrPutValue(m, 1, 1);
			}
		}

		GraphlyCount c = new GraphlyCount(map.keys(), map.values());
		log.info("Finished count of " + vids.length);
		return c;
	}

	@Override
	public long getRandomEdge(String graph, long v, long[] subset, Dir d)
			throws Exception {
		long[] edges = getEdges(graph, d, v);
		if (edges == null || edges.length == 0)
			return -1;

		if (subset.length == 0)
			return edges[(int) (Math.random() * edges.length)];
		else {
			TLongArrayList diff = new TLongArrayList(subset);
			TLongHashSet set = new TLongHashSet(edges);
			diff.retainAll(set);
			if (diff.isEmpty())
				return -1;
			return diff.get((int) (Math.random() * diff.size()));
		}
	}

	@Override
	public void setProperties(String graph, String to,
			TLongObjectHashMap<Object> m) throws Exception {
		TLongObjectIterator<Object> it = m.iterator();
		while (it.hasNext()) {
			it.advance();
			setProperty(graph, it.key(), to, it.value());
		}
	}

	@Override
	public TLongObjectHashMap<Object> getProperties(String graph, String k,
			int top, TLongArrayList list) throws Exception {
		if (top <= 0) {
			TLongObjectHashMap<Object> res = new TLongObjectHashMap<>();
			TLongIterator it = list.iterator();
			while (it.hasNext()) {
				long vid = it.next();
				res.put(vid, getProperty(graph, vid, k));
			}
			return res;
		}
		TLongObjectHashMap<Object> ret = new TLongObjectHashMap<Object>();

		TreeMultimap<Comparable, Long> sorted = TreeMultimap.create();
		TLongIterator it = list.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			Comparable value = (Comparable) getProperty(graph, vid, k);
			if (value != null) {
				if (sorted.size() < top) {
					sorted.put(value, vid);
				} else {
					Comparable toRemove = sorted.asMap().lastKey();
					if (toRemove.compareTo(value) < 0) {
						NavigableSet<Long> navigableSet = sorted.get(toRemove);
						long f = navigableSet.first();
						navigableSet.remove(f);
						sorted.put(value, vid);
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

	@Override
	public int getEdgeCount(String graph, long vid, Dir dir, long[] among)
			throws Exception {
		if (log.isDebugEnabled())
			log.debug("Getting edge count of vid among " + among.length);

		if (among == null || among.length == 0)
			return getEdges(graph, dir, vid).length;

		long[] curr = getEdges(graph, dir, vid);

		if (log.isDebugEnabled())
			log.debug("Intersecting " + among.length + " curr " + curr.length);
		if (curr == null || curr.length == 0)
			return 0;
		// Arrays.sort(among);
		int ret = GraphlyUtil.filter(among, curr).length;
		if (log.isDebugEnabled())
			log.debug("Returning intersection bt " + among.length + " curr "
					+ curr.length);
		return ret;
	}

	@Override
	public void setEdgeProperty(String graph, long v1, long v2, String k,
			Object val, String... labels) throws Exception {
	}

	@Override
	public Object getEdgeProperty(String graph, long v1, long v2, String k,
			String... labels) throws Exception {
		return null;
	}

	@Override
	public void setTempProperties(String graph,
			HashMap<Long, Map<String, Object>> temps) throws Exception {
		Map<Long, Map<String, Object>> map = this.temps.get(graph);
		if (map == null) {
			synchronized (temps) {
				map = this.temps.get(graph);
				if (map == null) {
					map = new ConcurrentHashMap<Long, Map<String, Object>>();
					this.temps.put(graph, map);
				}
			}
		}
		map.putAll(temps);
	}

	@Override
	public void commitUpdates(String graph, String[] k) throws Exception {
		Map<Long, Map<String, Object>> map = this.temps.get(graph);
		if (map != null) {
			for (Entry<Long, Map<String, Object>> e : map.entrySet()) {
				long vid = e.getKey();
				Map<String, Object> submap = e.getValue();
				for (String temp : k) {
					Object val = submap.get(temp);
					if (val != null)
						setProperty(graph, vid, temp, val);
				}
			}
			map.clear();
		}
	}

	@Override
	public Map<Long, Map<String, Object>> getProperties(String graph,
			long[] array, String... k) throws Exception {
		Map<Long, Map<String, Object>> ret = new HashMap<>();
		for (long l : array) {
			for (String propKey : k) {
				Map<String, Object> map = ret.get(l);
				if (map == null) {
					map = new HashMap<>();
					ret.put(l, map);
				}
				map.put(propKey, props.get(graph, l, propKey));
			}
		}
		return ret;
	}

	@Override
	public int getVertexCount(final String graph) throws Exception {
		return size_cache.get(graph, new Callable<Integer>() {

			@Override
			public Integer call() throws Exception {
				return store.count(buildVertexKey(graph, 0),
						buildVertexKey(graph, Long.MAX_VALUE));
			}
		});
	}

	@Override
	public TLongArrayList getVertices(String graph, long from, int lenght,
			boolean includeFirst) throws Exception {
		final int MAX_INIT_SIZE = 1000000;
		List<byte[]> list = store.getRangeOfLength(includeFirst,
				buildVertexKey(graph, from),
				buildVertexKey(graph, Long.MAX_VALUE), lenght);

		TLongArrayList ret = new TLongArrayList(Math.min(MAX_INIT_SIZE, lenght));
		for (byte[] bs : list)
			ret.add(DataTypeUtils.byteArrayToLong(bs));
		if (log.isDebugEnabled())
			log.debug("Returning list of vertices from " + ret.get(0) + "to"
					+ ret.get(ret.size() - 1));

		return ret;
	}

	@Override
	public Object getDefault(String graph, String k) throws Exception {
		return defaults.get(graph + "." + k);
	}

	@Override
	public void setDefault(String graph, String k, Object v) throws Exception {
		defaults.put(graph + "." + k, v);
	}

	@Override
	public synchronized double getDouble(String graph, long v, String k)
			throws Exception {
		double tObjectDoubleHashMap = doubleProps.get(graph, v, k);
		if (tObjectDoubleHashMap == doubleProps.NOT_FOUND)
			return getDefaultDouble(graph, k);
		return tObjectDoubleHashMap;
	}

	@Override
	public void setDouble(String graph, long v, String k, double currentVal)
			throws Exception {
		doubleProps.put(graph, v, k, currentVal);
	}

	@Override
	public void setDefaultDouble(String graph, String k, double v)
			throws Exception {
		defaultDoubleMap.put(graph + "." + k, v);
	}

	@Override
	public double getDefaultDouble(String graph, String k) throws Exception {
		return defaultDoubleMap.get(graph + "." + k);
	}

	@Override
	public Set<String> getGraphs() throws Exception {
		HashSet<String> ret = new HashSet<>();

		ByteBuffer from = new ByteBuffer(2);
		from.put(GRAPH);
		from.put((byte) 0);

		ByteBuffer to = new ByteBuffer(2);
		to.put(GRAPH);
		to.put((byte) 0xF);

		List<byte[]> res = store.getRangeOfLength(true, from.build(),
				to.build(), Integer.MAX_VALUE);

		for (byte[] bs : res) {
			ret.add(new String(bs));
		}

		return ret;
	}

	@Override
	public float getFloat(String graph, long v, String k) throws Exception {
		float tObjectDoubleHashMap = floatProps.get(graph, v, k);
		if (tObjectDoubleHashMap == floatProps.NOT_FOUND)
			return getDefaultFloat(graph, k);
		return tObjectDoubleHashMap;
	}

	@Override
	public void setFloat(String graph, long v, String k, float currentVal)
			throws Exception {
		floatProps.put(graph, v, k, currentVal);
	}

	@Override
	public void setDefaultFloat(String graph, String k, float v)
			throws Exception {
		defaultFloatMap.put(graph + "." + k, v);
	}

	@Override
	public float getDefaultFloat(String graph, String k) throws Exception {
		return defaultFloatMap.get(graph + "." + k);
	}

	@Override
	public Object gather(String graph, Gather<?> g) throws Exception {
		return g.gather(graph, this);
	}
}
