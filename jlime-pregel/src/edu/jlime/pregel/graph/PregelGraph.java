package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import edu.jlime.pregel.worker.VertexData;

public class PregelGraph implements Serializable {

	HashMap<String, Object> defaultValue = new HashMap<>();

	int id = 0;

	HashMap<Vertex, Set<Vertex>> outgoing = new HashMap<>();

	HashMap<Vertex, Set<Vertex>> incoming = new HashMap<>();

	List<Vertex> vertices = new ArrayList<>();

	private HashMap<Vertex, VertexData> data = new HashMap<>();

	public Vertex vertex() {
		Vertex vertex = new Vertex(id++);
		vertices.add(vertex);
		return vertex;
	}

	public void setDefaultValue(String k, Object defaultValue) {
		this.defaultValue.put(k, defaultValue);
	}

	public void putLink(Vertex o, Vertex dest) {
		Set<Vertex> list = outgoing.get(o);
		if (list == null) {
			list = new HashSet<>();
			outgoing.put(o, list);
		}
		list.add(dest);

		Set<Vertex> inc = incoming.get(dest);
		if (inc == null) {
			inc = new HashSet<>();
			incoming.put(dest, inc);
		}
		inc.add(o);

	}

	public void merge(PregelGraph result) {
		outgoing.putAll(result.outgoing);
		data.putAll(result.data);
		vertices.addAll(result.vertices);
	}

	public Set<Vertex> getOutgoing(Vertex vertex) {
		Set<Vertex> list = outgoing.get(vertex);
		if (list == null)
			return new HashSet<Vertex>();
		return list;
	}

	public void setVal(Vertex v, String k, Object val) {

		VertexData vData = this.data.get(v);
		if (vData == null) {
			vData = this.data.get(v);
			synchronized (data) {
				if (vData == null) {
					vData = new VertexData();
					this.data.put(v, vData);
				}
			}
		}

		vData.put(k, val);
	}

	public Boolean isTrue(Vertex v, String string) {
		Object vertexData = get(v, string);
		if (vertexData != null && vertexData instanceof Boolean)
			return (Boolean) vertexData;
		return false;
	}

	public void setVal(Vertex v, VertexData value) {
		for (Entry<String, Object> e : value.getData().entrySet())
			setVal(v, e.getKey(), e.getValue());
	}

	public void setTrue(Vertex v, String string) {
		setVal(v, string, new Boolean(true));
	}

	public void removeLink(Vertex v, Vertex toRemove) {
		Set<Vertex> list = outgoing.get(v);
		if (list != null)
			list.remove(toRemove);
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("Pregel Graph ID " + id + "\n");
		ret.append("Adyacency:\n");
		for (Entry<Vertex, Set<Vertex>> e : outgoing.entrySet()) {
			ret.append(e.getKey() + " -> " + e.getValue() + "\n");
		}
		ret.append("Data:\n");
		for (Entry<Vertex, VertexData> e : data.entrySet()) {
			ret.append(e.getKey() + " = " + e.getValue() + "\n");
		}
		ret.append("Vertices:\n");
		ret.append(vertices);
		return ret.toString();
	}

	public Object get(Vertex v, String k) {
		VertexData vData = this.data.get(v);
		Object data = null;
		if (vData != null)
			data = vData.getData(k);
		if (data == null)
			return defaultValue.get(k);
		return data;
	}

	public int vertexSize() {
		return vertices.size();
	}

	public List<Vertex> vertices() {
		return vertices;
	}

	public double getAdyacencySize(Vertex v) {
		Set<Vertex> list = getOutgoing(v);
		if (list != null)
			return list.size();
		return 0;
	}

	public VertexData getData(Vertex vertex) {
		return data.get(vertex);
	}

	public void addVertex(Vertex vertex) {
		vertices.add(vertex);
	}

	public Set<Vertex> getIncoming(Vertex v) {
		Set<Vertex> list = incoming.get(v);
		if (list == null)
			return new HashSet<Vertex>();
		return list;
	}
}
