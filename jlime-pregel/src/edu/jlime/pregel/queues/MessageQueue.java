package edu.jlime.pregel.queues;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;

public class MessageQueue implements ObjectMessageQueue {
	private static final int MAX = 5000000;
	private static final int AVGSIZE = 500;
	private volatile TreeMap<long[], Object> readOnly;
	private volatile TreeMap<long[], Object> current;
	private ObjectMessageMerger merger;
	private int contPut = 0;

	// TLongHashSet rO = new TLongHashSet();
	// TLongHashSet curr = new TLongHashSet();

	public MessageQueue(ObjectMessageMerger merger) {
		this.merger = merger;
		Comparator<long[]> comparator = new Comparator<long[]>() {
			MessageMerger merger = MessageQueue.this.merger;

			@Override
			public int compare(long[] o1, long[] o2) {
				int compare = Long.compare(o1[0], o2[0]);
				if (merger == null && compare == 0)
					return Long.compare(o1[1], o2[1]);
				return compare;
			}
		};
		readOnly = new TreeMap<>(comparator);
		current = new TreeMap<>(comparator);
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
	public synchronized void put(long from, long to, Object msg) {
		// GenericPregelMessage e = new GenericPregelMessage("", from, to, msg);
		long[] key = new long[] { to, from };
		if (merger == null)
			this.current.put(key, msg);
		else {
			Entry<long[], Object> found = this.current.ceilingEntry(key);
			if (found != null && found.getKey()[0] == to) {
				merger.merge(msg, found.getValue());
			} else {
				this.current.put(key, merger.getCopy(msg));
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
		TreeMap<long[], Object> aux = readOnly;
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

	@Override
	public void flush(String msgType, WorkerTask workerTask) throws Exception {
		// final Iterator<PregelMessage> it = readOnly.iterator();
		// while (it.hasNext()) {
		// PregelMessage msg = it.next();
		// workerTask.send(msg.getFrom(), msg.getTo(), msg.getV());
		// }
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType, long v) {
		SortedMap<long[], Object> sm = readOnly.subMap(new long[] { v,
				Long.MIN_VALUE }, new long[] { v + 1, Long.MIN_VALUE });
		final Iterator<Entry<long[], Object>> it = sm.entrySet().iterator();
		return new Iterator<PregelMessage>() {

			@Override
			public PregelMessage next() {
				Entry<long[], Object> e = it.next();
				long[] k = e.getKey();
				return new GenericPregelMessage(msgType, k[1], k[0],
						e.getValue());
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
		};
	}
}
