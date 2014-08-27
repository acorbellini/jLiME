package edu.jlime.pregel.worker;

import edu.jlime.pregel.graph.Vertex;

public class PregelMessage {

	Vertex v;

	VertexData data;

	public PregelMessage(Vertex from, VertexData data2) {
		this.v = from;
		this.data = data2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((v == null) ? 0 : v.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PregelMessage other = (PregelMessage) obj;
		if (v == null) {
			if (other.v != null)
				return false;
		} else if (!v.equals(other.v))
			return false;
		return true;
	}

	public Object get(String string) {
		return data.getData(string);
	}

	public VertexData getData() {
		return data;
	}

	public Vertex getVertex() {
		return v;
	}

	public boolean equals(String k, String val) {
		Object o = get(k);
		if (o == null)
			return false;
		return o.equals(val);
	}

}