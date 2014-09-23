package edu.jlime.pregel.worker;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VertexData implements Serializable {

	TLongArrayList incoming = null;

	TLongArrayList outgoing = null;

	private List<Object> data = null;

	private TLongArrayList disabledOut = null;

	private TLongArrayList disabledIn = null;

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

	public TLongList outgoing() {
		if (outgoing == null)
			return new TLongArrayList();
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

	public TLongList incoming() {
		if (incoming == null)
			return new TLongArrayList();
		return incoming;
	}

	public void outgoing(TLongSet other) {
		createOutgoing();
		this.outgoing.addAll(other);
	}

	private void createOutgoing() {
		if (outgoing == null)
			outgoing = new TLongArrayList();
	}

	public void incoming(TLongSet other) {
		createIncoming();
		this.incoming.addAll(other);
	}

	private void createIncoming() {
		if (incoming == null)
			incoming = new TLongArrayList();
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
				disabledOut = new TLongArrayList();
			disabledOut.add(to);
		}
	}

	public void disableIncoming(Long from) {
		if (incoming.contains(from)) {
			incoming.remove(from);
			if (disabledIn == null)
				disabledIn = new TLongArrayList();
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
		return "VertexData [incoming=" + incoming + ", outgoing=" + outgoing
				+ ", data=" + data + ", disabledOut=" + disabledOut
				+ ", disabledIn=" + disabledIn + "]";
	}

}
