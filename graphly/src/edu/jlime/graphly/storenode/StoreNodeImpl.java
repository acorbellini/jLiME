package edu.jlime.graphly.storenode;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.TreeMultimap;

import edu.jlime.core.rpc.RPC;
import edu.jlime.graphly.GraphlyConfig;
import edu.jlime.graphly.client.PropertyData;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.store.LocalStore;
import edu.jlime.graphly.storenode.properties.InMemoryGraphDoubleProperties;
import edu.jlime.graphly.storenode.properties.InMemoryGraphFloatProperties;
import edu.jlime.graphly.storenode.properties.InMemoryGraphProperties;
import edu.jlime.graphly.storenode.rpc.AdjacencyData;
import edu.jlime.graphly.storenode.rpc.StoreNode;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.Gather;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class StoreNodeImpl implements StoreNode {

	private static final long[] EMPTY_LONG_ARRAY = new long[] {};
	private static final byte VERTEX = 0x0;
	private static final byte ADJACENCY = 0x1;
	private static final byte VERTEX_PROP = 0x2;
	private static final byte GRAPH = 0x3;
	private static final byte EDGE_PROP = 0x4;
	private static final byte COUNT = 0x5;
	private static final byte FLOAT_PROPS = 0x6;

	Logger log = Logger.getLogger(StoreNodeImpl.class);

	Random random = new Random(System.currentTimeMillis());

	Semaphore sem = new Semaphore(2);
	private Map<String, Object> defaults = new ConcurrentHashMap<>();
	private TObjectDoubleHashMap<String> defaultDoubleMap = new TObjectDoubleHashMap<>();
	private Map<String, TObjectFloatHashMap<String>> defaultFloatMap = new ConcurrentHashMap<String, TObjectFloatHashMap<String>>();

	private LocalStore store;

	Cache<String, Boolean> graph_cache = CacheBuilder.newBuilder().maximumSize(100).build();

	ConcurrentHashMap<String, LoadingCache<Long, long[]>> adj_cache = new ConcurrentHashMap<>();

	Cache<Long, Boolean> vertex_cache = CacheBuilder.newBuilder().maximumSize(1000).build();

	Cache<String, Integer> size_cache = CacheBuilder.newBuilder().maximumSize(1000).build();

	private File localRanges;
	private List<Integer> ranges = new ArrayList<>();
	private InMemoryGraphProperties props = new InMemoryGraphProperties();
	private InMemoryGraphDoubleProperties doubleProps = new InMemoryGraphDoubleProperties();
	private InMemoryGraphFloatProperties floatProps = new InMemoryGraphFloatProperties();
	private InMemoryGraphFloatProperties tempFloatProps = new InMemoryGraphFloatProperties();

	private Map<String, Map<Long, Map<String, Object>>> temps = new ConcurrentHashMap<>();
	private GraphlyConfig config;

	// Store store;

	public StoreNodeImpl(String localpath, GraphlyConfig config, RPC rpc) throws IOException {

		this.config = config;

		Path path = Paths.get(localpath);
		if (!path.toFile().exists())
			Files.createDirectory(path);

		this.localRanges = new File(localpath + "/ranges.prop");
		if (!localRanges.exists())
			localRanges.createNewFile();

		Properties prop = new Properties();
		prop.load(new FileReader(localRanges));
		String rangeString = prop.getProperty("ranges");
		if (rangeString != null && !rangeString.isEmpty()) {
			rangeString = rangeString.replaceAll("\\[", "").replaceAll("\\s", "").replaceAll("\\]", "");
			for (String rangeVal : rangeString.split(",")) {
				ranges.add(Integer.valueOf(rangeVal));
			}
		}
		// this.graph = Neo4jGraph.open(localpath + "/neo4j");
		this.store = new LocalStore(localpath, config.storeCache, config.storePool);
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
	public void addEdges(String graph, long k, Dir type, long[] list) throws Exception {
		long id = k;

		id = getID(type, id);

		store.store(buildAdjacencyKey(graph, id), DataTypeUtils.longArrayToByteArray(list));

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

	private static byte[] buildCountKey(String graph) {
		byte[] gName = graph.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 4 + gName.length);
		buff.put(COUNT);
		buff.putByteArray(gName);
		return buff.build();
	}

	private static byte[] buildGraphKey(String graph) {
		byte[] gName = graph.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 4 + gName.length);
		buff.put(GRAPH);
		buff.putByteArray(gName);
		return buff.build();
	}

	private static byte[] buildAdjacencyKey(String graph, long id) {
		byte[] gName = graph.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 4 + gName.length + 8);
		buff.put(ADJACENCY);
		buff.putByteArray(gName);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private static byte[] buildVertexKey(String graph, long id) {
		byte[] gName = graph.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 4 + gName.length + 8);
		buff.put(VERTEX);
		buff.putString(graph);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	private static byte[] buildVertexPropertyKey(String graph, long id, String key) {
		byte[] b = key.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 8 + b.length);
		buff.put(VERTEX_PROP);
		buff.putString(graph);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		buff.putRawByteArray(b);
		return buff.build();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdges(edu.jlime.collections.
	 * adjacencygraph.get.GetType, java.lang.Long)
	 */
	@Override
	public long[] getEdges(String graph, Dir type, int max_edges, long[] id) throws ExecutionException {
		if (id.length == 1) {
			long[] edges = getEdges(graph, type, id[0]);
			if (edges.length > max_edges && max_edges > 0)
				edges = Arrays.copyOfRange(edges, 0, max_edges);
			return edges;
		}

		TLongHashSet ret = new TLongHashSet();
		for (long l : id) {
			long[] edges = getEdges(graph, type, l);
			if (edges.length > max_edges && max_edges > 0) {
				ret.addAll(Arrays.copyOfRange(edges, 0, max_edges));
			} else if (id.length > 1)
				ret.addAll(edges);
		}
		if (log.isDebugEnabled())
			log.debug("Returning " + ret.size());
		return ret.toArray();

	}

	private long[] getEdges(String graph, Dir type, long id) throws ExecutionException {
		if (type.equals(Dir.BOTH)) {
			long[] out = getEdges0(graph, getID(Dir.OUT, id));
			long[] in = getEdges0(graph, getID(Dir.IN, id));
			TLongHashSet list = new TLongHashSet(out.length + in.length);
			list.addAll(out);
			list.addAll(in);
			return list.toArray();
		}
		return getEdges0(graph, getID(type, id));
	}

	private long getID(Dir type, long id) {
		if (type.equals(Dir.IN))
			return -id - 1;
		return id;
	}

	@SuppressWarnings("unchecked")
	private long[] getEdges0(final String graph, long id) throws ExecutionException {
		if (config.edgeCacheType.equals("no-cache"))
			return loadEdgesFromStore(graph, id);

		LoadingCache<Long, long[]> cache = adj_cache.get(graph);
		if (cache == null) {
			synchronized (adj_cache) {
				cache = adj_cache.get(graph);
				if (cache == null) {

					// log.info("Max adjacency size: " + (size / (1024f *
					// 1024f))
					// + " MB");
					@SuppressWarnings("rawtypes")
					CacheBuilder builder = CacheBuilder.newBuilder();
					if (config.edgeCacheType.equals("mem-based")) {
						long size = (long) (Runtime.getRuntime().maxMemory() * config.cacheSize);
						builder = builder.maximumWeight(size).weigher(new Weigher<Long, long[]>() {

							@Override
							public int weigh(Long key, long[] value) {
								// Es lo que más ocupa en un heap
								// dump=>
								// 68
								// de softEntry y 68 de weightedEntry
								return 68 + 68 + 24 // size of Long
										+ 24 + value.length * 8;
							}
						}).softValues();
					} else if (config.edgeCacheType.equals("fixed-size")) {
						builder = builder.maximumSize(config.cacheLength);
					}
					cache = builder.build(new CacheLoader<Long, long[]>() {

						@Override
						public long[] load(Long key) throws Exception {
							return loadEdgesFromStore(graph, key);
						}

					});
					adj_cache.put(graph, cache);
				}
			}
		}
		return cache.get(id);

		// byte[] array;
		// try {
		// array = store.load(buildAdjacencyKey(graph, id));
		// if (array != null) {
		// long[] byteArrayToLongArray = DataTypeUtils
		// .byteArrayToLongArray(array);
		// return byteArrayToLongArray;
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		//
		// }
		// return new long[] {};

	}

	private long[] loadEdgesFromStore(final String graph, long key) {
		byte[] array;
		try {
			array = store.load(buildAdjacencyKey(graph, key));
			if (array != null) {
				long[] byteArrayToLongArray = DataTypeUtils.byteArrayToLongArray(array);
				return byteArrayToLongArray;
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		return EMPTY_LONG_ARRAY;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#setProperty(java.lang.Long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	public void setProperty(String graph, long vid, String k, Object val) throws Exception {
		if (config.persistObjects) {
			byte[] key = buildVertexPropertyKey(graph, vid, k);
			ByteBuffer buffer = new ByteBuffer();
			buffer.putObject(val);
			byte[] data = buffer.build();
			store.store(key, data);
		} else {
			props.put(graph, vid, k, val);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getProperty(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	public Object getProperty(String graph, long vid, String k) throws Exception {
		Object ret = null;
		if (config.persistObjects) {
			byte[] key = buildVertexPropertyKey(graph, vid, k);
			byte[] data = store.load(key);
			if (data != null)
				ret = new ByteBuffer(data).getObject();
		} else {
			ret = props.get(graph, vid, k);
		}
		if (ret == null)
			ret = getDefault(graph, k);
		return ret;
	}

	@Override
	public void addVertex(final String graph, final long id, final String label) throws Exception {
		vertex_cache.get(id, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				byte[] vk = buildVertexKey(graph, id);
				if (store.load(vk) == null) {
					byte[] labelAsBytes = label.getBytes();
					byte[] vertexData = new byte[8 + labelAsBytes.length];
					DataTypeUtils.longToByteArray(id, vertexData, 0);
					System.arraycopy(labelAsBytes, 0, vertexData, 8, labelAsBytes.length);
					store.store(vk, vertexData);
					synchronized (store) {
						byte[] buildCountKey = buildCountKey(graph);
						byte[] intBytes = store.load(buildCountKey);
						int toStore = 0;
						if (intBytes != null)
							toStore = DataTypeUtils.byteArrayToInt(intBytes);
						store.store(buildCountKey, DataTypeUtils.intToByteArray(toStore + 1));
					}

				}
				return true;
			}
		});
	}

	@Override
	public String getLabel(String graph, long id) throws Exception {
		byte[] vk = buildVertexKey(graph, id);
		byte[] data = store.load(vk);
		if (data != null) {
			ByteBuffer buffer = new ByteBuffer(data);
			buffer.getLong();
			return new String(buffer.getRawByteArray());
		}
		return null;
	}

	@Override
	public void addEdge(String graph, long orig, long dest, String label, Object[] keyValues) throws Exception {

		{
			long innerID = getID(Dir.OUT, orig);
			byte[] k = buildAdjacencyKey(graph, innerID);
			long[] adj = getEdges(graph, Dir.OUT, orig);
			if (Arrays.binarySearch(adj, dest) < 0) {
				adj = Arrays.copyOf(adj, adj.length + 1);
				adj[adj.length - 1] = dest;
				Arrays.sort(adj);
				store.store(k, DataTypeUtils.longArrayToByteArray(adj));
			}
		}

		{
			long innerID = getID(Dir.IN, dest);
			byte[] k = buildAdjacencyKey(graph, innerID);
			long[] adj = getEdges(graph, Dir.IN, dest);
			if (Arrays.binarySearch(adj, orig) < 0) {
				adj = Arrays.copyOf(adj, adj.length + 1);
				adj[adj.length - 1] = orig;
				Arrays.sort(adj);
				store.store(k, DataTypeUtils.longArrayToByteArray(adj));
			}
		}

		// addVertex(graph, orig, "");
		// addVertex(graph, dest, "");
		// addGraph(graph);
	}

	@Override
	public void removeVertex(String graph, long id) throws Exception {
	}

	@Override
	public void addInEdgePlaceholder(String graph, long id2, long id, String label) throws Exception {
	}

	@Override
	public Count countEdges(final String graph, final Dir dir, final int max_edges, final long[] keys,
			final float[] values, final long[] f) throws Exception {
		log.info("Counting edges in dir " + dir + " with max " + max_edges + " and vertices " + keys.length + ".");

		long[] toAdd = f == null ? EMPTY_LONG_ARRAY : f;
		final TLongHashSet toFilter = new TLongHashSet(toAdd);

		long init = System.currentTimeMillis();
		final int cores = Runtime.getRuntime().availableProcessors();

		ExecutorService exec = Executors.newFixedThreadPool(cores);

		int size = keys.length;
		final float chunks = (size / (float) cores);

		final TLongFloatMap finalResult = new TLongFloatHashMap(10000000);
		for (int i = 0; i < cores; i++) {
			final int tID = i;

			exec.execute(new Runnable() {
				@Override
				public void run() {

					int from = (int) (chunks * tID);
					int to = (int) (chunks * (tID + 1));

					if (tID == cores - 1)
						to = keys.length;
					long initChunk = System.currentTimeMillis();
					TLongFloatMap map = new TLongFloatHashMap();
					int cont = from;

					while (cont < to) {
						final long l = keys[cont];
						final float mult = values[cont];
						cont++;
						long[] curr = null;
						try {
							curr = getEdges(graph, dir, max_edges, new long[] { l });
						} catch (ExecutionException e1) {
							e1.printStackTrace();
						}
						if (curr.length > 500000) {
							synchronized (finalResult) {
								for (long m : curr)
									if (toFilter == null || !toFilter.contains(m))
										finalResult.adjustOrPutValue(m, mult, mult);
							}
						} else {
							for (long m : curr)
								if (toFilter == null || !toFilter.contains(m))
									map.adjustOrPutValue(m, mult, mult);
						}

					}
					log.info("Finished chunk of size " + (to - from) + " in "
							+ (System.currentTimeMillis() - initChunk));

					long initAdd = System.currentTimeMillis();
					synchronized (finalResult) {
						TLongFloatIterator itMap = map.iterator();
						while (itMap.hasNext()) {
							itMap.advance();
							finalResult.adjustOrPutValue(itMap.key(), itMap.value(), itMap.value());
						}
					}
					log.info("Finished adding results to final result in " + (System.currentTimeMillis() - initAdd));
				}
			});
		}

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		Count c = new Count(finalResult.keys(), finalResult.values());
		log.info("Finished count of " + keys.length + " (different) vertices resulting in " + finalResult.size()
				+ " vertices with counts in " + (System.currentTimeMillis() - init) + " ms");
		return c;

	}

	@Override
	public long getRandomEdge(String graph, long v, long[] subset, Dir d) throws Exception {
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
	public void setProperties(String graph, String to, TLongObjectMap<Object> m) throws Exception {
		TLongObjectIterator<Object> it = m.iterator();
		while (it.hasNext()) {
			it.advance();
			setProperty(graph, it.key(), to, it.value());
		}
	}

	@Override
	public TLongObjectHashMap<Object> getProperties(String graph, String k, int top, TLongArrayList list)
			throws Exception {
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
					Comparable toRemove = sorted.asMap().firstKey();
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
	public int getEdgeCount(String graph, long vid, Dir dir, TLongHashSet among) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Getting edge count of vid among " + among.size());

		if (among == null || among.size() == 0)
			return getEdges(graph, dir, vid).length;

		long[] curr = getEdges(graph, dir, vid);

		if (log.isDebugEnabled())
			log.debug("Intersecting " + among.size() + " curr " + curr.length);
		if (curr == null || curr.length == 0)
			return 0;
		// Arrays.sort(among);
		int ret = 0;
		for (long l : curr) {
			if (among.contains(l))
				ret++;
		}
		if (log.isDebugEnabled())
			log.debug("Returning intersection bt " + among.size() + " curr " + curr.length + ":" + ret);
		return ret;
	}

	@Override
	public void setEdgeProperty(String graph, long v1, long v2, String k, Object val, String... labels)
			throws Exception {
	}

	@Override
	public Object getEdgeProperty(String graph, long v1, long v2, String k, String... labels) throws Exception {
		return null;
	}

	@Override
	public void setTempProperties(String graph, HashMap<Long, Map<String, Object>> temps) throws Exception {
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
	public Map<Long, Map<String, Object>> getProperties(String graph, long[] array, String... k) throws Exception {
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
				byte[] buildCountKey = buildCountKey(graph);
				byte[] intBytes = store.load(buildCountKey);
				Integer ret = null;
				if (intBytes == null) {
					ret = store.count(buildVertexKey(graph, Long.MIN_VALUE), buildVertexKey(graph, Long.MAX_VALUE));
					store.store(buildCountKey, DataTypeUtils.intToByteArray(ret));
				} else
					ret = DataTypeUtils.byteArrayToInt(intBytes);
				return ret;
			}
		});
	}

	@Override
	public TLongArrayList getVertices(String graph, long from, int lenght, boolean includeFirst) throws Exception {
		final int MAX_INIT_SIZE = 1000000;
		List<byte[]> list = store.getRangeOfLength(includeFirst, buildVertexKey(graph, from),
				buildVertexKey(graph, Long.MAX_VALUE), lenght);

		TLongArrayList ret = new TLongArrayList(Math.min(MAX_INIT_SIZE, lenght));
		for (byte[] bs : list)
			ret.add(DataTypeUtils.byteArrayToLong(bs));
		if (log.isDebugEnabled())
			log.debug("Returning list of vertices from " + ret.get(0) + "to" + ret.get(ret.size() - 1));

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
	public synchronized double getDouble(String graph, long v, String k) throws Exception {
		double tObjectDoubleHashMap = doubleProps.get(graph, v, k);
		if (tObjectDoubleHashMap == doubleProps.NOT_FOUND)
			return getDefaultDouble(graph, k);
		return tObjectDoubleHashMap;
	}

	@Override
	public void setDouble(String graph, long v, String k, double currentVal) throws Exception {
		doubleProps.put(graph, v, k, currentVal);
	}

	@Override
	public void setDefaultDouble(String graph, String k, double v) throws Exception {
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

		List<byte[]> res = store.getRangeOfLength(true, from.build(), to.build(), Integer.MAX_VALUE);

		for (byte[] bs : res) {
			ret.add(new String(bs));
		}

		return ret;
	}

	@Override
	public float getFloat(String graph, long v, String k) throws Exception {
		if (config.persistfloats) {
			byte[] key = buildFloatPropertyKey(graph, v, k);
			byte[] val = store.load(key);
			if (val == null) {
				return getDefaultFloat(graph, k);
			}
			return Float.intBitsToFloat(DataTypeUtils.byteArrayToInt(val));
		} else {
			float tObjectDoubleHashMap = floatProps.get(graph, v, k);
			if (tObjectDoubleHashMap == InMemoryGraphFloatProperties.VALUE_NOT_FOUND)
				return getDefaultFloat(graph, k);
			return tObjectDoubleHashMap;
		}
	}

	@Override
	public void setFloat(String graph, long v, String k, float currentVal) throws Exception {
		if (config.persistfloats) {
			byte[] key = buildFloatPropertyKey(graph, v, k);
			store.store(key, DataTypeUtils.intToByteArray(Float.floatToIntBits(currentVal)));
		} else {
			floatProps.put(graph, v, k, currentVal);
		}
	}

	private static byte[] buildFloatPropertyKey(String g, long id, String k) {
		byte[] gName = g.getBytes();
		byte[] keyBytes = k.getBytes();
		ByteBuffer buff = new ByteBuffer(1 + 4 + gName.length + keyBytes.length + 8);
		buff.put(FLOAT_PROPS);
		buff.putByteArray(gName);
		buff.putByteArray(keyBytes);
		buff.putRawByteArray(DataTypeUtils.longToByteArrayOrdered(id));
		return buff.build();
	}

	@Override
	public void setDefaultFloat(String graph, String k, float v) throws Exception {
		TObjectFloatHashMap<String> gMap = defaultFloatMap.get(graph);
		if (gMap == null) {
			synchronized (defaultFloatMap) {
				gMap = defaultFloatMap.get(graph);
				if (gMap == null) {
					gMap = new TObjectFloatHashMap<>();
					defaultFloatMap.put(graph, gMap);
				}
			}
		}
		gMap.put(k, v);
	}

	@Override
	public float getDefaultFloat(String graph, String k) throws Exception {
		TObjectFloatHashMap<String> tObjectFloatHashMap = defaultFloatMap.get(graph);
		if (tObjectFloatHashMap == null)
			return 0f;
		return tObjectFloatHashMap.get(k);
	}

	@Override
	public Object gather(String graph, Gather<?> g) throws Exception {
		return g.gather(graph, this);
	}

	public void stop() {
		store.close();
	}

	@Override
	public void setTempFloats(String graph, String k, boolean add, long[] keys, float[] values) {
		if (add)
			this.tempFloatProps.addAll(graph, k, keys, values);
		else
			this.tempFloatProps.putAll(graph, k, keys, values);
	}

	@Override
	public void commitFloatUpdates(String graph, String... props) {

		for (String string : props) {
			floatProps.removeAll(graph, string);

			TLongFloatMap prop_vals = tempFloatProps.getAll(graph, string);
			synchronized (prop_vals) {
				floatProps.putAll(graph, string, prop_vals);
			}
			tempFloatProps.removeAll(graph, string);
		}
	}

	@Override
	public void updateFloatProperty(String graph, String prop, DivideUpdateProperty upd) throws Exception {
		TLongFloatMap map = floatProps.getAll(graph, prop);
		synchronized (map) {
			TLongFloatIterator it = map.iterator();
			while (it.hasNext()) {
				it.advance();
				it.setValue(upd.update(it.value()));
			}
		}
	}

	@Override
	public float getFloat(String graph, long v, String k, float alt) throws Exception {
		if (config.persistfloats) {
			byte[] key = buildFloatPropertyKey(graph, v, k);
			byte[] val = store.load(key);
			if (val == null) {
				return alt;
			}
			return Float.intBitsToFloat(DataTypeUtils.byteArrayToInt(val));
		} else {
			float tObjectDoubleHashMap = floatProps.get(graph, v, k);
			if (tObjectDoubleHashMap == InMemoryGraphFloatProperties.VALUE_NOT_FOUND)
				return alt;
			return tObjectDoubleHashMap;
		}
	}

	public TLongFloatIterator getFloatIterator(String graph, String k) throws Exception {
		if (config.persistfloats) {
			byte[] from = buildFloatPropertyKey(graph, Long.MIN_VALUE, k);
			byte[] to = buildFloatPropertyKey(graph, Long.MAX_VALUE, k);
			final Iterator<Pair<byte[], byte[]>> it = store.getRangeIterator(true, from, to, Integer.MAX_VALUE);
			return new TLongFloatIterator() {
				float val = 0f;
				private long key;

				@Override
				public void remove() {
				}

				@Override
				public boolean hasNext() {
					return it.hasNext();
				}

				@Override
				public void advance() {
					Pair<byte[], byte[]> next = it.next();
					key = DataTypeUtils.byteArrayToLongOrdered(next.left, next.left.length - 8);
					val = Float.intBitsToFloat(DataTypeUtils.byteArrayToInt(next.right));

				}

				@Override
				public float value() {
					return val;
				}

				@Override
				public float setValue(float val) {
					return 0;
				}

				@Override
				public long key() {
					return key;
				}
			};

		} else {
			TLongFloatMap tObjectDoubleHashMap = floatProps.getAll(graph, k);
			return tObjectDoubleHashMap.iterator();
		}
	}

	@Override
	public void setFloats(String graph, String k, TLongFloatMap subProp) throws Exception {
		TLongFloatIterator it = subProp.iterator();
		while (it.hasNext()) {
			it.advance();
			setFloat(graph, it.key(), k, it.value());
		}

	}

	@Override
	public void setProperty(String graph, String k, String val, TLongArrayList value) throws Exception {
		TLongIterator it = value.iterator();
		while (it.hasNext())
			setProperty(graph, it.next(), k, val);
	}

	public void addFloat(String graph, long v, String k, float f) throws Exception {
		if (config.persistfloats) {
			byte[] key = buildFloatPropertyKey(graph, v, k);
			float curr = getFloat(graph, v, k, 0f);
			store.store(key, DataTypeUtils.intToByteArray(Float.floatToIntBits(curr + f)));
		} else {
			floatProps.add(graph, v, k, f);
		}
	}

	@Override
	public Map<String, PropertyData> getAllProperties(String graph, long[] array) {
		Map<String, PropertyData> ret = new HashMap<>();
		for (String propKey : props.getProperties()) {
			TLongObjectMap<Object> tlo = new TLongObjectHashMap<>();
			for (long l : array) {
				tlo.put(l, props.get(graph, l, propKey));
			}
			PropertyData data = new PropertyData(tlo.keys(), tlo.values());
			ret.put(propKey, data);
		}
		return ret;
	}

	@Override
	public Map<String, TLongFloatMap> getAllFloatProperties(String graph, long[] array) throws Exception {
		Map<String, TLongFloatMap> ret = new HashMap<>();
		for (String propKey : floatProps.getProperties()) {
			TLongFloatMap tlo = new TLongFloatHashMap();
			for (long l : array) {
				tlo.put(l, floatProps.get(graph, l, propKey));
			}
			ret.put(propKey, tlo);
		}
		return ret;
	}

	@Override
	public AdjacencyData getAllEdges(String graph, TLongArrayList value, Dir dir) throws Exception {
		TLongObjectHashMap<long[]> ret = new TLongObjectHashMap<>();
		TLongIterator it = value.iterator();
		while (it.hasNext()) {
			long v = it.next();
			ret.put(v, getEdges(graph, dir, v));
		}
		return new AdjacencyData(ret.keys(), ret.values(new long[ret.size()][]));
	}

	public boolean containsVertex(String g, long v) throws Exception {
		return store.containsKey(buildVertexKey(g, v));
	}

	@Override
	public void createSubgraph(String graph, String sg, long[] vids) throws Exception {
		TLongHashSet sg_list = new TLongHashSet(vids);
		for (long l : vids) {
			if (containsVertex(graph, l)) {
				{
					long[] edges = getEdges(graph, Dir.IN, l, sg_list);
					if (edges != null)
						addEdges(sg, l, Dir.IN, edges);
				}
				{
					long[] edges = getEdges(graph, Dir.OUT, l, sg_list);
					if (edges != null)
						addEdges(sg, l, Dir.OUT, edges);
				}
			}
		}
	}

	private long[] getEdges(String g, Dir dir, long v, TLongHashSet vertices) throws Exception {
		long[] edges = getEdges(g, dir, v);
		if (edges == null)
			return null;
		TLongHashSet ret = new TLongHashSet();
		for (long l : edges) {
			if (vertices.contains(l))
				ret.add(l);
		}
		return ret.toArray();
	}

}
