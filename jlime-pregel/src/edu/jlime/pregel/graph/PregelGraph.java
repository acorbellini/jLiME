package edu.jlime.pregel.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PregelGraph {
	int id = 0;

	HashMap<Vertex, List<Vertex>> adyacency = new HashMap<>();

	public Vertex vertex() {
		return new Vertex(this,id++);
	}

	public void putLink(Vertex o, Vertex d) {
		List<Vertex> list = adyacency.get(o);
		if (list == null) {
			list = new ArrayList<>();
			adyacency.put(o, list);
		}
		list.add(d);
	}
}
