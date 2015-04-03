package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;

public class MessageQueue {
	private static final int MAX = 5000000;
	private static final int AVGSIZE = 500;
	private volatile PersistentOrderedQueue readOnly;
	private volatile PersistentOrderedQueue current;

	// TLongHashSet rO = new TLongHashSet();
	// TLongHashSet curr = new TLongHashSet();

	public MessageQueue(MessageMerger merger) {
		this.readOnly = new PersistentOrderedQueue(merger, MAX, AVGSIZE);
		this.current = new PersistentOrderedQueue(merger, MAX, AVGSIZE);
	}

	public void put(PregelMessage pregelMessage) {
		this.current.put(pregelMessage);
	}

	public synchronized void switchQueue() {
		PersistentOrderedQueue aux = readOnly;
		this.readOnly = current;
		this.current = aux;
		this.current.clear();
	}

	public int currentSize() {
		return current.size();
	}

	public int readOnlySize() {
		return readOnly.size();
	}

	public Iterator<List<PregelMessage>> iterator() {
		return readOnly.iterator();
	}

}
