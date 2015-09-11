package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.worker.VertexData;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class PregelGraphLocal implements Serializable, PregelGraph {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6834962501460416222L;

	boolean createOriginVerticesOnly;

	Map<String, Object> defaultValue = new ConcurrentHashMap<String, Object>();

	private static long id = 0l;

	long graphid = 0;

	TLongObjectHashMap<VertexData> vertices = new TLongObjectHashMap<VertexData>(1024, 0.75f);

	TLongObjectHashMap<VertexData> disabled = new TLongObjectHashMap<VertexData>(1024, 0.75f);

	private String name;

	public PregelGraphLocal(String name, boolean origin) {
		this.graphid = id++;
		this.createOriginVerticesOnly = origin;
		this.name = name;
	}

	// HashMap<byte[], Object> data = new HashMap<>();

	// HashMap<byte[], Long[]> incoming = new HashMap<>();

	// HashMap<byte[], Long[]> outcoming = new HashMap<>();

	// TreeSet<byte[]> incoming = new TreeSet<>(new LongLongComparator());

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.jlime.pregel.graph.PregelGraphAbstract#setDefaultValue(java.lang.
	 * String, java.lang.Object)
	 */
	@Override
	public void setDefaultValue(String k, Object defaultValue) {
		this.defaultValue.put(k, defaultValue);
	}

	private VertexData getOrCreateVData(long id) {
		VertexData data = getVertexData(id);
		if (data == null) {
			synchronized (vertices) {
				if (data == null) {
					data = new VertexData();
					vertices.put(id, data);
				}
			}

		}
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#putLink(Long, Long)
	 */
	@Override
	public void putOutgoing(long o, long dest) {
		// System.out.println("Putting " + o + "->" + dest + " on graph "
		// + graphid);

		getOrCreateVData(o).outgoing(dest);

		// ByteBuffer buff = new ByteBuffer(16);
		// buff.putLong(o);
		// buff.putLong(dest);
		// incoming.add(buff.build());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.jlime.pregel.graph.PregelGraphAbstract#merge(edu.jlime.pregel.graph
	 * .PregelGraph)
	 */
	// @Override
	// public void merge(PregelGraph result) {
	// TLongObjectIterator<VertexData> it = ((PregelGraphLocal) result).vertices
	// .iterator();
	// while (it.hasNext()) {
	// it.advance();
	// long v = it.key();
	// VertexData data = it.value();
	// VertexData vData = null;
	// TLongSet outgoing = data.outgoing();
	// if (outgoing.size() != 0) {
	// if (vData == null)
	// vData = getOrCreateVData(v);
	// vData.outgoing(outgoing);
	// }
	//
	// HashMap<String, Object> vertexData = data.getData();
	// if (vertexData != null && vertexData.size() != 0) {
	// if (vData == null)
	// vData = getOrCreateVData(v);
	// vData.putAll(vertexData);
	// }
	//
	// }
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#getOutgoing(Long)
	 */
	@Override
	public TLongHashSet getOutgoing(long vertex) {
		VertexData data = getVertexData(vertex);
		if (data == null)
			return new TLongHashSet();
		return data.outgoing();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#setVal(Long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	public void setVal(long v, String k, Object val) {
		getOrCreateVData(v).put(k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#isTrue(Long,
	 * java.lang.String)
	 */
	// @Override
	// public Boolean isTrue(long v, String string) {
	// VertexData vertexData = getVertexData(v);
	// if (vertexData == null)
	// return false;
	// else
	// return vertexData.isTrue(string);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#setVal(Long,
	 * edu.jlime.pregel.worker.VertexData)
	 */
	// @Override
	// public void setVal(long v, VertexData value) {
	// HashMap<String, Object> data = value.getData();
	// if (data != null)
	// for (Entry<String, Object> e : data.entrySet())
	// setVal(v, e.getKey(), e.getValue());
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#setTrue(Long,
	 * java.lang.String)
	 */
	// @Override
	// public void setTrue(long v, String string) {
	// setVal(v, string, new Boolean(true));
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#removeLink(Long, Long)
	 */
	@Override
	public void removeOutgoing(long from, long to) {
		VertexData data = getVertexData(from);
		if (data == null)
			return;
		data.removeOutgoing(to);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#toString()
	 */
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("Pregel Graph ID " + graphid + "\n");
		return ret.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#print()
	 */
	// @Override
	public String print() {
		synchronized (vertices) {
			StringBuilder ret = new StringBuilder();
			ret.append("Adyacency:\n");
			TLongObjectIterator<VertexData> it = vertices.iterator();
			while (it.hasNext()) {
				it.advance();
				ret.append(it.key() + " -> " + it.value().outgoing() + "\n");
			}
			ret.append("Data:\n");

			it = vertices.iterator();
			while (it.hasNext()) {
				it.advance();
				ret.append(it.key() + " = " + it.value().getData() + "\n");
			}
			ret.append("Vertices:\n");
			ret.append(vertices);
			return ret.toString();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#get(Long,
	 * java.lang.String)
	 */
	@Override
	public Object get(long v, String k) {
		VertexData vData = getVertexData(v);
		Object data = null;
		if (vData != null)
			data = vData.getData(k);
		if (data == null)
			data = getDefaultValue(k);
		return data;
	}

	private VertexData getVertexData(long v) {
		synchronized (vertices) {
			return this.vertices.get(v);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#vertexSize()
	 */
	@Override
	public int vertexSize() {
		synchronized (vertices) {
			return vertices.size();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#vertices()
	 */
	@Override
	public Set<Long> vertices() {
		return new HashSet<Long>(new TLongSetDecorator(vertices.keySet()));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#getAdyacencySize(Long)
	 */
	@Override
	public int getAdyacencySize(long v) {
		VertexData data = getVertexData(v);
		if (data != null)
			return data.outgoingSize();
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#getData(Long)
	 */
	// @Override
	// public VertexData getData(long vertex) {
	// return getVertexData(vertex);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#addVertex(Long)
	 */
	// @Override
	// public void addVertex(long vertex) {
	// getOrCreateVData(vertex);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#getIncoming(Long)
	 */
	@Override
	public TLongHashSet getIncoming(long v) {

		VertexData data = getVertexData(v);
		if (data == null)
			return new TLongHashSet();

		return data.incoming();
		// ByteBuffer from = new ByteBuffer(16);
		// from.putLong(v);
		// from.putLong(0);
		//
		// ByteBuffer to = new ByteBuffer(16);
		// to.putLong(v);
		// to.putLong(Long.MAX_VALUE);
		//
		// Iterator<byte[]> it = incoming.subSet(from.build(), to.build())
		// .iterator();
		//
		// HashSet<Long> ret = new HashSet<>();
		// while (it.hasNext()) {
		// ByteBuffer buff = new ByteBuffer(it.next());
		// long currV = buff.getLong();
		// if (currV != v)
		// return ret;
		// ret.add(buff.getLong());
		// }
		// return ret;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.jlime.pregel.graph.PregelGraphAbstract#getDefault(java.lang.String)
	 */
	@Override
	public Object getDefaultValue(String key) {
		return defaultValue.get(key);
	}

	@Override
	public int getOutgoingSize(long v) throws Exception {
		return getOutgoing(v).size();
	}

	@Override
	public void putIncoming(long o, long dest) throws Exception {
		getOrCreateVData(o).incoming(dest);
	}

	@Override
	public void disable(long v) throws Exception {
		disabled.put(v, vertices.remove(v));
	}

	@Override
	public void enableAll() throws Exception {
		synchronized (vertices) {
			vertices.putAll(disabled);
		}

		disabled.clear();
		synchronized (vertices) {
			TLongObjectIterator<VertexData> it = vertices.iterator();
			while (it.hasNext()) {
				it.advance();
				it.value().enableAll();
			}
		}
	}

	@Override
	public void putLink(long o, long dest) throws Exception {
		putOutgoing(o, dest);
		putIncoming(dest, o);
	}

	@Override
	public void disableLink(long v, long from) throws Exception {
		disableOutgoing(v, from);
		disableIncoming(from, v);
	}

	@Override
	public void disableOutgoing(long v, long from) throws Exception {
		VertexData data = getVertexData(v);
		if (data == null) {
			data = disabled.get(v);
			if (data == null)
				return;
		}

		data.disableOutgoing(from);

	}

	@Override
	public void disableIncoming(long to, long from) throws Exception {
		VertexData data = getVertexData(to);
		if (data == null) {
			data = disabled.get(to);
			if (data == null)
				return;
		}

		data.disableIncoming(from);

	}

	@Override
	public String getName() throws Exception {
		return name;
	}

	@Override
	public boolean createVertex(long from) {
		getOrCreateVData(from);
		return true;
	}

	@Override
	public void putOutgoing(List<long[]> value) throws Exception {
		for (long[] longs : value) {
			putOutgoing(longs[0], longs[1]);
		}
	}

	@Override
	public void createVertices(Set<Long> value) {
		for (long l : value) {
			createVertex(l);
		}
	}

	@Override
	public double getDouble(long v, String string) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setDouble(long v, String string, double currentVal) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public float getFloat(long v, String string) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setFloat(long v, String string, float currentVal) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public float getDefaultFloat(String prop) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String string, long v, float f) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TLongHashSet getNeighbours(long v) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNeighbourhoodSize(long v) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TLongHashSet getAdjacents(long v, Dir dir) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
