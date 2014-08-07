package edu.jlime.core.cluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Peer implements Serializable {

	private static final long serialVersionUID = -1369404741700349661L;

	public static final String NAME = "NAME";

	private String id;

	private String name;

	private HashMap<String, String> data = new HashMap<>();

	public Peer(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getID() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void putData(String k, String d) {
		data.put(k, d);
	}

	public void putData(Map<String, String> data) {
		data.putAll(data);
	}

	public String getData(String k) {
		return data.get(k);
	}

	public HashMap<String, String> getDataMap() {
		return data;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Peer))
			return false;
		Peer srv = (Peer) obj;
		return id.equals(srv.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return name + " " + id;
	}
}