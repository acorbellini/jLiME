package edu.jlime.pregel.worker;

import java.io.Serializable;
import java.util.HashMap;

public class VertexData implements Serializable {

	private HashMap<String, Object> data = new HashMap<>();

	public void put(String k, Object val) {
		data.put(k, val);
	}

	public Object getData(String k) {
		return data.get(k);
	}

	public HashMap<String, Object> getData() {
		return data;
	}

	public void setData(HashMap<String, Object> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "VertexData [data=" + data + "]";
	}

	public static VertexData create(String k, Object val) {
		VertexData ret = new VertexData();
		ret.put(k, val);
		return ret;
	}

}
