package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.DoublePregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongDoubleHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DoubleHashedMessageQueue implements PregelMessageQueue {

	private volatile TLongDoubleHashMap readOnly = new TLongDoubleHashMap(8,
			.75f, Long.MAX_VALUE, Double.MAX_VALUE);
	private volatile TLongDoubleHashMap current = new TLongDoubleHashMap(8,
			.75f, Long.MAX_VALUE, Double.MAX_VALUE);
	private DoubleMessageMerger merger;

	public DoubleHashedMessageQueue(DoubleMessageMerger merger) {
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
	public synchronized void putDouble(long from, long to, double msg) {
		double found = this.current.get(to);
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
		TLongDoubleHashMap aux = readOnly;
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
			final TLongDoubleIterator it = readOnly.iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				it.advance();
				ret.add(new DoublePregelMessage(-1, it.key(), it.value()));
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
			this.putDouble(from, to, 0f);
		else
			this.putDouble(from, to, (Double) val);
	}

	@Override
	public void flush(WorkerTask workerTask) throws Exception {
		HashMap<Worker, TLongArrayList> keys = new HashMap<>();
		HashMap<Worker, TDoubleArrayList> values = new HashMap<>();

		final TLongDoubleIterator it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			long to = it.key();

			if (to == -1) {
				workerTask.outputDouble(-1l, -1l, it.value());
			} else {
				Worker w = workerTask.getWorker(to);
				TLongArrayList keyList = keys.get(w);
				if (keyList == null) {
					keyList = new TLongArrayList();
					keys.put(w, keyList);
				}
				keyList.add(to);

				TDoubleArrayList valList = values.get(w);
				if (valList == null) {
					valList = new TDoubleArrayList();
					values.put(w, valList);
				}
				valList.add(it.value());
			}
			it.remove();
		}

		workerTask.sendDoubles(keys, values);

	}

	@Override
	public void putFloat(long from, long to, float val) {
		putDouble(from, to, (float) val);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final long to) {
		final double found = this.readOnly.get(to);
		if (found == this.readOnly.getNoEntryValue())
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new DoublePregelMessage(-1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

}
