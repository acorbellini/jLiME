package edu.jlime.pregel.worker;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VertexData implements Serializable {

	TLongHashSet incoming = null;

	TLongHashSet outgoing = null;

	private Map<String, Object> data = null;

	private TLongHashSet disabledOut = new TLongHashSet(2, 0.9f);

	private TLongHashSet disabledIn = new TLongHashSet(2, 0.9f);

	public void put(String k, Object val) {
		if (data == null)
			data = new ConcurrentHashMap<>();
		data.put(k, val);
	}

	public Object getData(String k) {
		if (data == null)
			return null;
		return data.get(k);
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(HashMap<String, Object> data) {
		this.data = data;
	}

	public void outgoing(long dest) {
		createOutgoing();
		outgoing.add(dest);
	}

	public void incoming(long from) {
		createIncoming();
		incoming.add(from);
	}

	public TLongSet outgoing() {
		if (outgoing == null)
			return new TLongHashSet();
		return outgoing;
	}

	public Boolean isTrue(String string) {
		Object d = data.get(string);
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

	public TLongSet incoming() {
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
			outgoing = new TLongHashSet(2, 0.9f);
	}

	public void incoming(TLongSet other) {
		createIncoming();
		this.incoming.addAll(other);
	}

	private void createIncoming() {
		if (incoming == null)
			incoming = new TLongHashSet(2, 0.9f);
	}

	public void putAll(HashMap<String, Object> vertexData) {
		if (data == null)
			data = new HashMap<>();
		data.putAll(vertexData);
	}

	public void disableOutgoing(Long to) {
		outgoing.remove(to);
		disabledOut.add(to);
	}

	public void disableIncoming(Long from) {
		incoming.remove(from);
		disabledIn.add(from);
	}

	public void enableAll() {
		if (outgoing != null && !disabledOut.isEmpty()) {
			outgoing.addAll(disabledOut);
			disabledOut.clear();
		}
		if (incoming != null && !disabledIn.isEmpty()) {
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
