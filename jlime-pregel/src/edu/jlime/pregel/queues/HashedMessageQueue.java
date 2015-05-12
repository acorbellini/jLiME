package edu.jlime.pregel.queues;

import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HashedMessageQueue implements PregelMessageQueue {
	private volatile TLongObjectHashMap<Object> readOnly = new TLongObjectHashMap<>();
	private volatile TLongObjectHashMap<Object> current = new TLongObjectHashMap<>();
	private ObjectMessageMerger merger;

	public HashedMessageQueue(ObjectMessageMerger merger) {
		this.merger = merger;
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
		Object found = this.current.get(to);
		if (found != null) {
			this.current.put(to, merger.merge(found, msg));
		} else {
			this.current.put(to, msg);
		}
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#iterator()
	 */
	@Override
	public Iterator<List<PregelMessage>> iterator() {
		// Collections.sort(readOnly);

		return new Iterator<List<PregelMessage>>() {
			final TLongObjectIterator<Object> it = readOnly.iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				it.advance();
				ret.add(new GenericPregelMessage(-1, it.key(), it.value()));
				return ret;
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
		};
	}

	@Override
	public void putFloat(long from, long to, float val) {
		this.put(from, to, val);
	}

	@Override
	public void flush(WorkerTask workerTask) throws Exception {
		final TLongObjectIterator<Object> it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			workerTask.outputObject(-1, it.key(), it.value());
			it.remove();
		}
	}

	@Override
	public void putDouble(long from, long to, double val) {
		this.put(from, to, val);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final long to) {
		final Object found = this.current.get(to);
		if (found == null)
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new GenericPregelMessage(-1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}
}
