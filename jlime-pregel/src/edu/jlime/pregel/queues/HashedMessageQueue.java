package edu.jlime.pregel.queues;

import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.Iterator;

public class HashedMessageQueue implements ObjectMessageQueue {
	private volatile TLongObjectHashMap<Object> readOnly = new TLongObjectHashMap<>();
	private volatile TLongObjectHashMap<Object> current = new TLongObjectHashMap<>();
	private ObjectMessageMerger merger;

	public HashedMessageQueue(ObjectMessageMerger merger) {
		this.merger = merger;
	}

	public synchronized void put(long from, long to, Object msg) {
		Object found = this.current.get(to);
		if (found != null) {
			merger.merge(msg, found);
		} else
			this.current.put(to, merger.getCopy(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#switchQueue()
	 */
	@Override
	public synchronized void switchQueue() {
		TLongObjectHashMap<Object> aux = readOnly;
		this.readOnly = current;
		this.current = aux;
		this.current.clear();
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
		final TLongObjectIterator<Object> it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			workerTask.outputObject(msgType, -1, it.key(), it.value());
			it.remove();
		}
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		final Object found = this.readOnly.get(to);
		if (found == null)
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new GenericPregelMessage(msgType, -1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}
}
