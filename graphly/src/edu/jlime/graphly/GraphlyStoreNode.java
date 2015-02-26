package edu.jlime.graphly;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.TreeMultimap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.store.LocalStore;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyStoreNode implements GraphlyStoreNodeI {

	Logger log = Logger.getLogger(GraphlyStoreNode.class);

	private static class InMemoryGraphProperties {
		ConcurrentHashMap<Long, Map<String, Object>> props = new ConcurrentHashMap<>();

		public void put(Long vid, String k, Object val) {
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

		public Object get(Long vid, String k) {
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
	private LocalStore adj;

	Cache<Long, long[]> adj_cache = CacheBuilder.newBuilder().maximumSize(5000)
			.build();

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
		this.adj = new LocalStore(name, localpath);
	}

	@Override
	public List<Integer> getRanges() {
		return ranges;
	}

	@Override
	public synchronized void addRange(Integer range) throws IOException {
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
	public void addEdges(Long id, Dir type, long[] list) throws Exception {
		if (type.equals(Dir.IN))
			id = -id - 1;
		adj.store(id, DataTypeUtils.longArrayToByteArray(list));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdges(edu.jlime.collections.
	 * adjacencygraph.get.GetType, java.lang.Long)
	 */
	@Override
	public long[] getEdges(Dir type, Integer max_edges, long[] id)
			throws ExecutionException {
		if (log.isDebugEnabled())
			log.debug("Obtaining edges");
		TLongArrayList ret = new TLongArrayList();
		for (long l : id) {
			if (log.isDebugEnabled())
				log.debug("Processing vid " + l);
			long[] edges = getEdges(type, l);
			if (log.isDebugEnabled())
				log.debug("Got processing vid " + edges);

			if (edges.length > max_edges && max_edges > 0) {

				if (log.isDebugEnabled())
					log.debug("Filtering " + edges + " getting only "
							+ max_edges);
				int in = 0;
				for (in = 0; in < edges.length && ret.size() < max_edges; in++) {
					int rn = edges.length - in;
					int rm = max_edges - ret.size();
					if (Math.random() * rn < rm)
						/* Take it */
						ret.add(edges[in]); /*
											 * +1 since your range begins from 1
											 */
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

	private long[] getEdges0(final Long id) throws ExecutionException {
		return adj_cache.get(id, new Callable<long[]>() {

			@Override
			public long[] call() throws Exception {
				byte[] array;
				try {
					array = adj.load((int) id.longValue());
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
	public void setProperty(Long vid, String k, Object val) throws Exception {
		props.put(vid, k, val);
		// Iterator<Vertex> v = graph.iterators().vertexIterator(vid);
		// Vertex v1 = null;
		// if (!v.hasNext()) {
		// addVertex(vid, null);
		// v = graph.iterators().vertexIterator(vid);
		// }
		// if (v.hasNext())
		// v1 = v.next();
		// if (v1 != null)
		// v1.property(k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getProperty(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	public Object getProperty(Long vid, String k) throws Exception {
		return props.get(vid, k);
		// Iterator<Vertex> v = graph.iterators().vertexIterator(vid);
		// Vertex v1 = null;
		// if (!v.hasNext()) {
		// addVertex(vid, null);
		// v = graph.iterators().vertexIterator(vid);
		// }
		// if (v.hasNext())
		// v1 = v.next();
		// if (v1 != null)
		// return v1.property(k);
		// return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#setEdgeProperty(java.lang.Long,
	 * java.lang.Long, java.lang.String, java.lang.Object, java.lang.String)
	 */
	// @Override
	// public void setEdgeProperty(Long v1, Long v2, String k, Object val,
	// String... labels) {
	// Edge edge = getEdge(v1, v2, labels);
	// if (edge != null) {
	// edge.property(k, val);
	// }
	// }

	// private Edge getEdge(Long v1, Long v2, String... labels) {
	// List<Edge> edges = getEdges(v1, Dir.OUT, labels);
	// for (Edge edge : edges) {
	// if (edge.outV().next().equals(v2)) {
	// return edge;
	// }
	// }
	// return null;
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdgeProperty(java.lang.Long,
	 * java.lang.Long, java.lang.String, java.lang.String)
	 */
	// @Override
	// public Object getEdgeProperty(Long v1, Long v2, String k, String...
	// labels) {
	// Edge e = getEdge(v1, v2, labels);
	// return e.property(k);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdge(java.lang.Long,
	 * edu.jlime.collections.adjacencygraph.get.GetType, java.lang.String)
	 */
	// @Override
	// public List<Edge> getEdges(Long orig, Dir type, String... labels) {
	// List<Edge> edges = new ArrayList<>();
	// Vertex v1 = graph.V(orig).next();
	// if (v1 != null) {
	// Direction dir = Direction.IN;
	// if (type.equals(GraphlyDirection.BOTH))
	// dir = Direction.BOTH;
	// else if (type.equals(GraphlyDirection.OUT))
	// dir = Direction.OUT;
	// for (Edge edge : v1.toE(dir, labels).toList())
	// edges.add(edge);
	// }
	// return edges;
	// return null;
	// }

	@Override
	public boolean addVertex(Long id, String label) throws Exception {
		// if (label != null) {
		// Vertex v = graph.addVertex(T.id, id, T.label, label);
		// return v == null;
		// } else {
		// Vertex v = graph.addVertex(T.id, id);
		// return v == null;
		// }
		return false;
	}

	@Override
	public String getLabel(Long id) throws Exception {
		// return graph.V(id).next().label();
		return null;
	}

	@Override
	public void addEdge(Long orig, Long dest, String label, Object[] keyValues)
			throws Exception {
	}

	@Override
	public void removeVertex(Long id) throws Exception {
		// graph.V(id).next().remove();
	}

	@Override
	public void addInEdgePlaceholder(Long id2, Long id, String label)
			throws Exception {
		// graph.V(id2).next().addEdge(label, graph.V(id).next());
	}

	public void setJobExecutorID(Peer je) {
		this.je = je;
	}

	@Override
	public Peer getJobAddress() throws Exception {
		return je;
	}

	@Override
	public GraphlyCount countEdges(Dir dir, Integer max_edges, long[] vids)
			throws Exception {
		TLongIntHashMap map = new TLongIntHashMap();
		for (long l : vids) {
			long[] curr = getEdges(dir, max_edges, new long[] { l });
			for (long m : curr) {
				map.adjustOrPutValue(m, 1, 1);
			}
		}
		GraphlyCount c = new GraphlyCount(map.keys(), map.values());
		return c;
	}

	@Override
	public Long getRandomEdge(Long v, long[] subset, Dir d) throws Exception {
		long[] edges = getEdges(d, v);
		if (edges == null || edges.length == 0)
			return null;

		if (subset.length == 0)
			return edges[(int) (Math.random() * edges.length)];
		else {
			TLongArrayList diff = new TLongArrayList(subset);
			TLongHashSet set = new TLongHashSet(edges);
			diff.retainAll(set);
			if (diff.isEmpty())
				return null;
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
	public TLongObjectHashMap<Object> getProperties(String k, Integer top,
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
						Long f = navigableSet.first();
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
	public int getEdgeCount(Long vid, Dir dir, long[] among) throws Exception {
		if (among == null || among.length == 0)
			return getEdges(dir, vid).length;
		long[] curr = getEdges(dir, vid);
		if (curr == null || curr.length == 0)
			return 0;
		// int cont = 0;
		//
		// int edgesCur = 0;
		// int amongCur = 0;
		// while (edgesCur < curr.length && amongCur < among.length) {
		// if (among[amongCur] == curr[edgesCur]) {
		// cont++;
		// edgesCur++;
		// amongCur++;
		// } else if (among[amongCur] > curr[edgesCur])
		// edgesCur++;
		// else
		// amongCur++;
		// }
		// return cont;
		long[] smaller = among;
		long[] bigger = curr;
		if (among.length > curr.length) {
			smaller = curr;
			bigger = among;
		}
		TLongHashSet aux = new TLongHashSet(smaller);
		aux.retainAll(bigger);
		return aux.size();

	}

	@Override
	public void setEdgeProperty(Long v1, Long v2, String k, Object val,
			String... labels) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getEdgeProperty(Long v1, Long v2, String k, String... labels)
			throws Exception {
		// TODO Auto-generated method stub
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
			Long vid = e.getKey();
			Map<String, Object> map = e.getValue();
			for (String temp : k) {
				Object val = map.get(temp);
				if (val != null)
					setProperty(vid, temp, val);
			}
		}
		temps.clear();
	}
}
