package edu.jlime.graphly;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.tinkerpop.gremlin.structure.Edge;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.collections.intintarray.db.LevelDb;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;

public class GraphlyStoreNode implements GraphlyStoreNodeI {

	// private Graph graph;
	private LevelDb adj;
	private File localRanges;
	private List<Integer> ranges = new ArrayList<>();
	private Peer je;

	// Store store;

	public GraphlyStoreNode(String name, String localpath, RPCDispatcher rpc)
			throws IOException {
		File dir = new File(localpath);
		dir.mkdir();
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

		// this.graph = Neo4jGraph.open(localpath);
		this.adj = new LevelDb(name, localpath);
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
		if (type.equals(Dir.OUT))
			id = -id;
		adj.store(id, DataTypeUtils.longArrayToByteArray(list));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdges(edu.jlime.collections.
	 * adjacencygraph.get.GetType, java.lang.Long)
	 */
	@Override
	public long[] getEdges(Dir type, long[] id) {
		TLongArrayList ret = new TLongArrayList();
		for (long l : id) {
			ret.addAll(getEdges(type, l));
		}

		return ret.toArray();

	}

	private long[] getEdges(Dir type, long id) {
		if (type.equals(Dir.BOTH)) {
			TLongArrayList list = new TLongArrayList();
			list.addAll(getEdges0(id));
			list.addAll(getEdges0(-id));
			return list.toArray();
		}

		if (type.equals(Dir.OUT))
			id = -id;

		return getEdges0(id);
	}

	private long[] getEdges0(Long id) {
		byte[] array;
		try {
			array = adj.load((int) id.longValue());
			return DataTypeUtils.byteArrayToLongArray(array);
		} catch (Exception e) {
			return new long[] {};
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#setProperty(java.lang.Long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	public void setProperty(Long vid, String k, Object val) {
		// Vertex v1 = graph.V(vid).next();
		// v1.property(k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getProperty(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	public Object getProperty(Long vid, String k) {
		// Vertex v1 = graph.V(vid).next();
		// return v1.property(k);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#setEdgeProperty(java.lang.Long,
	 * java.lang.Long, java.lang.String, java.lang.Object, java.lang.String)
	 */
	@Override
	public void setEdgeProperty(Long v1, Long v2, String k, Object val,
			String... labels) {
		Edge edge = getEdge(v1, v2, labels);
		if (edge != null) {
			edge.property(k, val);
		}
	}

	private Edge getEdge(Long v1, Long v2, String... labels) {
		List<Edge> edges = getEdges(v1, Dir.OUT, labels);
		for (Edge edge : edges) {
			if (edge.outV().next().equals(v2)) {
				return edge;
			}
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdgeProperty(java.lang.Long,
	 * java.lang.Long, java.lang.String, java.lang.String)
	 */
	@Override
	public Object getEdgeProperty(Long v1, Long v2, String k, String... labels) {
		Edge e = getEdge(v1, v2, labels);
		return e.property(k);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdge(java.lang.Long,
	 * edu.jlime.collections.adjacencygraph.get.GetType, java.lang.String)
	 */
	@Override
	public List<Edge> getEdges(Long orig, Dir type, String... labels) {
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
		return null;
	}

	@Override
	public boolean addVertex(Long id, String label) throws Exception {
		// Vertex v = graph.addVertex(T.id, id, T.label, label);
		// return v == null;
		return true;
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
	public TLongIntHashMap countEdges(Dir dir, long[] vids) throws Exception {
		TLongIntHashMap map = new TLongIntHashMap();
		for (long l : vids) {
			long[] curr = getEdges(dir, l);
			for (long m : curr) {
				map.adjustOrPutValue(m, 1, 1);
			}
		}
		return map;
	}

	@Override
	public Long getRandomEdge(Long v, Dir d) throws Exception {
		long[] edges = getEdges(d, v);
		if (edges == null || edges.length == 0)
			return null;
		return edges[(int) (Math.random() * edges.length)];
	}
}
