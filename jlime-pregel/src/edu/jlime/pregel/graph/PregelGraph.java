package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import edu.jlime.pregel.worker.VertexData;

public class PregelGraph implements Serializable {

	int id = 0;

	HashMap<Vertex, List<Vertex>> adyacency = new HashMap<>();

	private HashMap<Vertex, VertexData> data = new HashMap<>();

	public Vertex vertex() {
		return new Vertex(id++);
	}

	public void putLink(Vertex o, Vertex dest) {
		List<Vertex> list = adyacency.get(o);
		if (list == null) {
			list = new ArrayList<>();
			adyacency.put(o, list);
		}
		list.add(dest);
	}

	public void merge(PregelGraph result) {
		adyacency.putAll(result.adyacency);
	}

	public List<Vertex> getAdyacency(Vertex vertex) {
		List<Vertex> list = adyacency.get(vertex);
		if (list == null)
			return new ArrayList<Vertex>();
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

	public boolean isTrue(Vertex v, String string) {
		VertexData vertexData = data.get(v);
		if (vertexData != null) {
			Object data2 = vertexData.getData(string);
			if (data2 != null)
				return (Boolean) data2;
		}
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
		List<Vertex> list = adyacency.get(v);
		if (list != null)
			list.remove(toRemove);
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("Pregel Graph ID " + id + "\n");
		ret.append("Adyacency:\n");
		for (Entry<Vertex, List<Vertex>> e : adyacency.entrySet()) {
			ret.append(e.getKey() + " -> " + e.getValue() + "\n");
		}
		ret.append("Data:\n");
		for (Entry<Vertex, VertexData> e : data.entrySet()) {
			ret.append(e.getKey() + " = " + e.getValue() + "\n");
		}
		return ret.toString();
	}

}
