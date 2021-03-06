package edu.jlime.pregel.worker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class VertexData implements Serializable {

	TLongHashSet incoming = null;

	TLongHashSet outgoing = null;

	private List<Object> data = null;

	private TLongHashSet disabledOut = null;

	private TLongHashSet disabledIn = null;

	public void put(String k, Object val) {
		createData();
		for (int i = 0; i < data.size(); i += 2) {
			if (((String) data.get(i)).equals(k)) {
				data.set(i + 1, val);
				return;
			}
		}
		data.add(k);
		data.add(val);

	}

	public Object getData(String k) {
		if (data == null)
			return null;
		for (int i = 0; i < data.size(); i += 2) {
			if (((String) data.get(i)).equals(k)) {
				return data.get(i + 1);
			}
		}
		return null;
	}

	public Map<String, Object> getData() {
		HashMap<String, Object> map = new HashMap<>();
		if (data != null)
			for (int i = 0; i < data.size(); i += 2) {
				map.put((String) data.get(i), data.get(i + 1));
			}
		return map;
	}

	public void outgoing(long dest) {
		createOutgoing();
		if (!outgoing.contains(dest))
			outgoing.add(dest);
	}

	public void incoming(long from) {
		createIncoming();
		if (!incoming.contains(from))
			incoming.add(from);
	}

	public TLongHashSet outgoing() {
		if (outgoing == null)
			return new TLongHashSet();
		return outgoing;
	}

	public Boolean isTrue(String string) {
		Object d = getData(string);
		if (d != null && d instanceof Boolean)
			return (Boolean) d;
		return false;
	}

	public void removeOutgoing(long to) {
		if (outgoing != null)
			outgoing.remove(to);

	}

	public int outgoingSize() {
		if (outgoing != null)
			return outgoing.size();
		return 0;
	}

	public TLongHashSet incoming() {
		if (incoming == null)
			return new TLongHashSet();
		return incoming;
	}

	public void outgoing(TLongSet other) {
		createOutgoing();
		this.outgoing.addAll(other);
	}

	private void createOutgoing() {
		if (outgoing == null)
			outgoing = new TLongHashSet();
	}

	public void incoming(TLongSet other) {
		createIncoming();
		this.incoming.addAll(other);
	}

	private void createIncoming() {
		if (incoming == null)
			incoming = new TLongHashSet();
	}

	// public void putAll(HashMap<String, Object> vertexData) {
	// createData();
	// data.putAll(vertexData);
	// }

	private void createData() {
		if (data == null)
			data = new ArrayList<>();
	}

	public void disableOutgoing(Long to) {
		if (outgoing.contains(to)) {
			outgoing.remove(to);
			if (disabledOut == null)
				disabledOut = new TLongHashSet();
			disabledOut.add(to);
		}
	}

	public void disableIncoming(Long from) {
		if (incoming.contains(from)) {
			incoming.remove(from);
			if (disabledIn == null)
				disabledIn = new TLongHashSet();
			disabledIn.add(from);
		}
	}

	public void enableAll() {
		if (outgoing != null && disabledOut != null && !disabledOut.isEmpty()) {
			outgoing.addAll(disabledOut);
			disabledOut.clear();
		}
		if (incoming != null && disabledIn != null && !disabledIn.isEmpty()) {
			incoming.addAll(disabledIn);
			disabledIn.clear();
		}
	}

	@Override
	public String toString() {
		return "VertexData [incoming=" + incoming + ", outgoing=" + outgoing + ", data=" + data + ", disabledOut="
				+ disabledOut + ", disabledIn=" + disabledIn + "]";
	}

}
