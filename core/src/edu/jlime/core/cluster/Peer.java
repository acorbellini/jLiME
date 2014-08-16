package edu.jlime.core.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import edu.jlime.core.transport.Address;
import edu.jlime.util.StreamUtils;

public class Peer implements Externalizable, Comparable<Peer> {

	private static final long serialVersionUID = -1369404741700349661L;

	public static final String NAME = "NAME";

	private Address address;

	private String name;

	private Map<String, String> data = new HashMap<>();

	public Peer() {
	}

	public Peer(Address address, String name) {
		this(address, name, new HashMap<String, String>());
	}

	public Peer(Address addr, String name, Map<String, String> map) {
		this.name = name;
		this.address = addr;
		this.data = map;
	}

	public String getName() {
		return name;
	}

	public void putData(String k, String d) {
		data.put(k, d);
	}

	public void putData(Map<String, String> data) {
		this.data.putAll(data);
	}

	public String getData(String k) {
		return data.get(k);
	}

	public Map<String, String> getDataMap() {
		return data;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Peer))
			return false;
		Peer srv = (Peer) obj;
		return address.equals(srv.address);
	}

	@Override
	public int hashCode() {
		return address.hashCode();
	}

	@Override
	public String toString() {
		return name + " " + address;
	}

	@Override
	public int compareTo(Peer o) {
		int compareTo = this.name.compareTo(o.name);
		if (compareTo == 0)
			return this.address.compareTo(o.address);
		return compareTo;
	}

	public Address getAddress() {
		return address;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(address);
		StreamUtils.putString(out, name);
		StreamUtils.putMap(out, data);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.address = (Address) in.readObject();
		this.name = StreamUtils.readString(in);
		this.data = StreamUtils.readMap(in);
	}
}