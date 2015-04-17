package edu.jlime.pregel.worker;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FloatHashedMessageQueue implements PregelMessageQueue {

	private volatile TLongFloatHashMap readOnly = new TLongFloatHashMap(8,
			.8f, Long.MAX_VALUE, Float.MAX_VALUE);
	private volatile TLongFloatHashMap current = new TLongFloatHashMap(8,
			.8f, Long.MAX_VALUE, Float.MAX_VALUE);
	private FloatMessageMerger merger;

	public FloatHashedMessageQueue(FloatMessageMerger merger) {
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
	public synchronized void putFloat(long from, long to, float msg) {
		float found = this.current.get(to);
		if (found == this.current.getNoEntryValue()) {
			this.current.put(to, msg);
		} else {
			this.current.put(to, merger.merge(found, msg));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.pregel.worker.PregelMessageQueue#switchQueue()
	 */
	@Override
	public synchronized void switchQueue() {
		TLongFloatHashMap aux = readOnly;
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
			final TLongFloatIterator it = readOnly.iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				it.advance();
				ret.add(new FloatPregelMessage(-1, it.key(), it.value()));
				return ret;
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
		};
	}

	@Override
	public void put(long from, long to, Object val) {
		if (val == null)
			this.putFloat(from, to, 0f);
		else
			this.putFloat(from, to, (Float) val);
	}

	@Override
	public void flush(WorkerTask workerTask) throws Exception {
		final TLongFloatIterator it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			workerTask.outputFloat(-1, it.key(), it.value());
			it.remove();
		}
	}

}
