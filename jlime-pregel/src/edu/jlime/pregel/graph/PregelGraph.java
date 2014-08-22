package edu.jlime.pregel.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PregelGraph {

	int id = 0;

	HashMap<Vertex, List<Vertex>> adyacency = new HashMap<>();

	private HashMap<Vertex, byte[]> values = new HashMap<>();

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
		return adyacency.get(vertex);
	}

	public void setVal(Vertex v, byte[] bs) {
		this.values.put(v, bs);

	}
}
