package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class MessageQueue implements PregelMessageQueue {
	private static final int MAX = 5000000;
	private static final int AVGSIZE = 500;
	private volatile TreeSet<PregelMessage> readOnly;
	private volatile TreeSet<PregelMessage> current;
	private ObjectMessageMerger merger;
	private int contPut = 0;

	// TLongHashSet rO = new TLongHashSet();
	// TLongHashSet curr = new TLongHashSet();

	public MessageQueue(ObjectMessageMerger merger) {
		this.merger = merger;
		Comparator<PregelMessage> comparator = new Comparator<PregelMessage>() {
			MessageMerger merger = MessageQueue.this.merger;

			@Override
			public int compare(PregelMessage o1, PregelMessage o2) {
				if (merger != null)
					return Long.compare(o1.to, o2.to);
				return o1.compareTo(o2);
			}
		};
		readOnly = new TreeSet<PregelMessage>(comparator);
		current = new TreeSet<PregelMessage>(comparator);
		// this.readOnly = new PersistentOrderedQueue(merger, MAX, AVGSIZE);
		// this.current = new PersistentOrderedQueue(merger, MAX, AVGSIZE);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.jlime.pregel.worker.PregelMessageQueue#put(edu.jlime.pregel.worker
	 * .PregelMessage)
	 */
	@Override
	public synchronized void put(long from, long to, Object msg) {
		GenericPregelMessage e = new GenericPregelMessage(from, to, msg);
		if (merger == null)
			this.current.add(e);
		else {
			PregelMessage found = this.current.ceiling(e);
			if (found != null && found.getTo() == to) {
				found.setV(merger.merge(found.getV(), msg));
			} else {
				this.current.add(e);
			}
		}

		// contPut++;
		// if (merger != null && contPut > MAX) {
		// compact();
		// contPut = 0;
		// }
	}

	//
	// public synchronized void compact() {
	// if (merger == null)
	// return;
	// Collections.sort(current);
	// PregelMessage last = null;
	// Iterator<PregelMessage> it = current.iterator();
	// while (it.hasNext()) {
	// PregelMessage msg = (PregelMessage) it.next();
	// if (last == null) {
	// last = msg;
	// } else {
	// if (msg.getTo() == last.getTo()) {
	// Object val = merger.merge(last.getV(), msg.getV());
	// last.setV(val);
	// it.remove();
	// } else {
	// last = msg;
	// }
	// }
	// }
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#switchQueue()
	 */
	@Override
	public synchronized void switchQueue() {
		TreeSet<PregelMessage> aux = readOnly;
		this.readOnly = current;
		this.current = aux;
		this.current.clear();
		contPut = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#currentSize()
	 */
	@Override
	public int currentSize() {
		return current.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#readOnlySize()
	 */
	@Override
	public int readOnlySize() {
		return readOnly.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#iterator()
	 */
	@Override
	public Iterator<List<PregelMessage>> iterator() {
		// Collections.sort(readOnly);

		return new Iterator<List<PregelMessage>>() {
			PregelMessage curr;
			final Iterator<PregelMessage> it = readOnly.iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				if (curr != null) {
					ret.add(curr);
					curr = null;
				}
				while (it.hasNext()) {
					curr = it.next();
					if (ret.isEmpty()) {
						ret.add(curr);
						curr = null;
					} else {
						if (ret.get(0).to != curr.to)
							return ret;
						else {
							ret.add(curr);
							curr = null;
						}
					}
				}
				return ret;
			}

			@Override
			public boolean hasNext() {
				return curr != null || it.hasNext();
			}
		};
	}

	@Override
	public void putFloat(long from, long to, float val) {
		this.put(from, to, val);
	}

	@Override
	public void flush(WorkerTask workerTask) throws Exception {
		final Iterator<PregelMessage> it = readOnly.iterator();
		while (it.hasNext()) {
			PregelMessage msg = it.next();
			workerTask.send(msg.getFrom(), msg.getTo(), msg.getV());
		}
	}
}
