package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.VertexData;
import gnu.trove.decorator.TLongListDecorator;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.TLongList;
import gnu.trove.map.hash.TLongObjectHashMap;

public class PregelGraphLocal implements Serializable, Graph {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6834962501460416222L;

	boolean createOriginVerticesOnly;

	Map<String, Object> defaultValue = new ConcurrentHashMap<String, Object>();

	private static long id = 0l;

	long graphid = 0;

	TLongObjectHashMap<VertexData> vertices = new TLongObjectHashMap<VertexData>(
			1024, 0.75f);

	TLongObjectHashMap<VertexData> disabled = new TLongObjectHashMap<VertexData>(
			1024, 0.75f);

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

	private VertexData getOrCreateVData(Long id) {
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
	public void putOutgoing(Long o, Long dest) {
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
	// Long v = it.key();
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
	public Set<Long> getOutgoing(Long vertex) {
		VertexData data = getVertexData(vertex);
		if (data == null)
			return new HashSet<Long>();

		HashSet<Long> ret = new HashSet<Long>();
		TLongList outgoing = data.outgoing();
		TLongIterator it = outgoing.iterator();
		while (it.hasNext()) {
			ret.add(it.next());
		}
		return ret;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#setVal(Long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	public void setVal(Long v, String k, Object val) {
		getOrCreateVData(v).put(k, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#isTrue(Long,
	 * java.lang.String)
	 */
	// @Override
	// public Boolean isTrue(Long v, String string) {
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
	// public void setVal(Long v, VertexData value) {
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
	// public void setTrue(Long v, String string) {
	// setVal(v, string, new Boolean(true));
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#removeLink(Long, Long)
	 */
	@Override
	public void removeOutgoing(Long from, Long to) {
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
	public Object get(Long v, String k) {
		VertexData vData = getVertexData(v);
		Object data = null;
		if (vData != null)
			data = vData.getData(k);
		if (data == null)
			data = getDefaultValue(k);
		return data;
	}

	private VertexData getVertexData(Long v) {
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
	public int getAdyacencySize(Long v) {
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
	// public VertexData getData(Long vertex) {
	// return getVertexData(vertex);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#addVertex(Long)
	 */
	// @Override
	// public void addVertex(Long vertex) {
	// getOrCreateVData(vertex);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.graph.PregelGraphAbstract#getIncoming(Long)
	 */
	@Override
	public Set<Long> getIncoming(Long v) {

		VertexData data = getVertexData(v);
		if (data == null)
			return new HashSet<Long>();

		HashSet<Long> ret = new HashSet<Long>();
		TLongList outgoing = data.incoming();
		TLongIterator it = outgoing.iterator();
		while (it.hasNext()) {
			ret.add(it.next());
		}
		return ret;
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
		// Long currV = buff.getLong();
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
	public int getOutgoingSize(Long v) throws Exception {
		return getOutgoing(v).size();
	}

	@Override
	public void putIncoming(Long o, Long dest) throws Exception {
		getOrCreateVData(o).incoming(dest);
	}

	@Override
	public void disable(Long v) throws Exception {
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
	public void putLink(Long o, Long dest) throws Exception {
		putOutgoing(o, dest);
		putIncoming(dest, o);
	}

	@Override
	public void disableLink(Long v, Long from) throws Exception {
		disableOutgoing(v, from);
		disableIncoming(from, v);
	}

	@Override
	public void disableOutgoing(Long v, Long from) throws Exception {
		VertexData data = getVertexData(v);
		if (data == null) {
			data = disabled.get(v);
			if (data == null)
				return;
		}

		data.disableOutgoing(from);

	}

	@Override
	public void disableIncoming(Long to, Long from) throws Exception {
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
	public boolean createVertex(Long from) {
		getOrCreateVData(from);
		return true;
	}

	@Override
	public void putOutgoing(List<Long[]> value) throws Exception {
		for (Long[] longs : value) {
			putOutgoing(longs[0], longs[1]);
		}
	}

	@Override
	public void createVertices(Set<Long> value) {
		for (Long l : value) {
			createVertex(l);
		}
	}
}