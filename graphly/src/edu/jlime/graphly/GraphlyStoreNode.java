package edu.jlime.graphly;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import com.google.common.base.Defaults;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.TreeMultimap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.store.LocalStore;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyStoreNode implements GraphlyStoreNodeI {

	private static byte VERTEX = 0x0;
	private static byte ADJACENCY = 0x1;
	private static byte VERTEX_PROP = 0x2;
	private static byte EDGE_PROP = 0x3;

	Logger log = Logger.getLogger(GraphlyStoreNode.class);

	private static class InMemoryGraphProperties {
		ConcurrentHashMap<Long, Map<String, Object>> props = new ConcurrentHashMap<>();

		public void put(long vid, String k, Object val) {
			Map<String, Object> map = props.get(vid);
			if (map == null) {
				synchronized (props) {
					map = props.get(vid);
					if (map == null) {
						map = new ConcurrentHashMap<String, Object>();
						props.put(vid, map);
					}
				}
			}
			map.put(k, val);
		}

		public Object get(long vid, String k) {
			Map<String, Object> map = props.get(vid);
			if (map != null) {
				return map.get(k);
			}
			return null;
		}

		@Override
		public String toString() {
			return props.toString();
		}
	}

	// private Graph graph;
	private LocalStore store;

	Cache<Long, long[]> adj_cache = CacheBuilder.newBuilder().maximumSize(5000)
			.build();

	Cache<Long, Boolean> vertex_cache = CacheBuilder.newBuilder()
			.maximumSize(5000).build();

	private File localRanges;
	private List<Integer> ranges = new ArrayList<>();
	private Peer je;
	private InMemoryGraphProperties props = new InMemoryGraphProperties();

	private Map<Long, Map<String, Object>> temps = new ConcurrentHashMap<>();

	// Store store;

	public GraphlyStoreNode(String name, String localpath, RPCDispatcher rpc)
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
		this.store = new LocalStore(name, localpath);
	}

	@Override
	public List<Integer> getRanges() {
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
	public void addEdges(long k, Dir type, long[] list) throws Exception {
		long id = k;

		if (type.equals(Dir.IN))
			id = -id - 1;

		store.store(buildAdjacencyKey(id),
				DataTypeUtils.longArrayToByteArray(list));

		addVertex(k, "");
		// for (long l : list) {
		// addVertex(l, "");
		// }
	}

	private byte[] buildAdjacencyKey(long id) {
		ByteBuffer buff = new ByteBuffer(1 + 8);
		buff.put(ADJACENCY);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private byte[] buildVertexKey(long id) {
		ByteBuffer buff = new ByteBuffer(1 + 8);
		buff.put(VERTEX);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private byte[] buildVertexPropertyKey(long id, String key) {
		byte[] b = key.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 8 + b.length);
		buff.put(VERTEX_PROP);
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
	public long[] getEdges(Dir type, int max_edges, long[] id)
			throws ExecutionException {
		TLongArrayList ret = new TLongArrayList();
		for (long l : id) {
			long[] edges = getEdges(type, l);
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

	private long[] getEdges(Dir type, long id) throws ExecutionException {
		if (type.equals(Dir.BOTH)) {
			TLongHashSet list = new TLongHashSet();
			list.addAll(getEdges0(id));
			list.addAll(getEdges0(-id - 1));
			return list.toArray();
		}

		if (type.equals(Dir.IN))
			id = -id - 1;

		return getEdges0(id);
	}

	private long[] getEdges0(final long id) throws ExecutionException {
		return adj_cache.get(id, new Callable<long[]>() {
			@Override
			public long[] call() throws Exception {
				byte[] array;
				try {
					array = store.load(buildAdjacencyKey(id));
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
	public void setProperty(long vid, String k, Object val) throws Exception {
		props.put(vid, k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getProperty(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	public Object getProperty(long vid, String k) throws Exception {
		return props.get(vid, k);
	}

	@Override
	public void addVertex(final long id, String label) throws Exception {
		vertex_cache.get(id, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				byte[] vk = buildVertexKey(id);
				if (store.load(vk) == null)
					store.store(vk, DataTypeUtils.longToByteArray(id));
				return true;
			}
		});
	}

	@Override
	public String getLabel(long id) throws Exception {
		return null;
	}

	@Override
	public void addEdge(long orig, long dest, String label, Object[] keyValues)
			throws Exception {
	}

	@Override
	public void removeVertex(long id) throws Exception {
	}

	@Override
	public void addInEdgePlaceholder(long id2, long id, String label)
			throws Exception {
	}

	Semaphore sem = new Semaphore(2);
	private Map<String, Object> defaults = new ConcurrentHashMap<>();

	@Override
	public GraphlyCount countEdges(Dir dir, int max_edges, long[] vids)
			throws Exception {
		log.info("Counting edges in dir " + dir + " with max " + max_edges
				+ " and vertices " + vids.length + ".");
		TLongIntHashMap map = new TLongIntHashMap();
		// Serializing in this spot leads to amazing performance
		// Don't really know why, I guess has something to do with cache lines.
		long[][] res = new long[vids.length][];
		int cont = 0;
		synchronized (this) {
			for (long l : vids) {
				long[] curr = getEdges(dir, max_edges, new long[] { l });
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
	public long getRandomEdge(long v, long[] subset, Dir d) throws Exception {
		long[] edges = getEdges(d, v);
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
	public void setProperties(String to, TLongObjectHashMap<Object> m)
			throws Exception {
		TLongObjectIterator<Object> it = m.iterator();
		while (it.hasNext()) {
			it.advance();
			setProperty(it.key(), to, it.value());
		}
	}

	@Override
	public TLongObjectHashMap<Object> getProperties(String k, int top,
			TLongArrayList list) throws Exception {
		if (top <= 0) {
			TLongObjectHashMap<Object> res = new TLongObjectHashMap<>();
			TLongIterator it = list.iterator();
			while (it.hasNext()) {
				long vid = it.next();
				res.put(vid, getProperty(vid, k));
			}
			return res;
		}
		TLongObjectHashMap<Object> ret = new TLongObjectHashMap<Object>();

		TreeMultimap<Comparable, Long> sorted = TreeMultimap.create();
		TLongIterator it = list.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			Comparable value = (Comparable) getProperty(vid, k);
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
	public int getEdgeCount(long vid, Dir dir, long[] among) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Getting edge count of vid among " + among.length);

		if (among == null || among.length == 0)
			return getEdges(dir, vid).length;

		long[] curr = getEdges(dir, vid);

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
	public void setEdgeProperty(long v1, long v2, String k, Object val,
			String... labels) throws Exception {
	}

	@Override
	public Object getEdgeProperty(long v1, long v2, String k, String... labels)
			throws Exception {
		return null;
	}

	@Override
	public void setTempProperties(HashMap<Long, Map<String, Object>> temps)
			throws Exception {
		this.temps.putAll(temps);
	}

	@Override
	public void commitUpdates(String[] k) throws Exception {
		for (Entry<Long, Map<String, Object>> e : temps.entrySet()) {
			long vid = e.getKey();
			Map<String, Object> map = e.getValue();
			for (String temp : k) {
				Object val = map.get(temp);
				if (val != null)
					setProperty(vid, temp, val);
			}
		}
		temps.clear();
	}

	@Override
	public Map<Long, Map<String, Object>> getProperties(long[] array,
			String... k) throws Exception {
		Map<Long, Map<String, Object>> ret = new HashMap<>();
		for (long l : array) {
			for (String propKey : k) {
				Map<String, Object> map = ret.get(l);
				if (map == null) {
					map = new HashMap<>();
					ret.put(l, map);
				}
				map.put(propKey, props.get(l, propKey));
			}
		}
		return ret;
	}

	@Override
	public int getVertexCount() throws Exception {
		return store.count(buildVertexKey(0), buildVertexKey(Long.MAX_VALUE));
	}

	@Override
	public TLongArrayList getVertices(long from, int lenght,
			boolean includeFirst) throws Exception {
		List<byte[]> list = store.getRangeOfLength(includeFirst,
				buildVertexKey(from), buildVertexKey(Long.MAX_VALUE), lenght);
		TLongArrayList ret = new TLongArrayList(lenght);
		for (byte[] bs : list)
			ret.add(DataTypeUtils.byteArrayToLong(bs));
		return ret;
	}

	@Override
	public Object getDefault(String k) throws Exception {
		return defaults.get(k);
	}

	@Override
	public void setDefault(String k, Object v) throws Exception {
		defaults.put(k, v);
	}
}
