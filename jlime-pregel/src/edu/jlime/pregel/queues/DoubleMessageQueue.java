package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.DoublePregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.HashMap;
import java.util.Iterator;

public class DoubleMessageQueue implements PregelMessageQueue {

	private volatile TLongDoubleHashMap readOnly = new TLongDoubleHashMap(8,
			.75f, Long.MAX_VALUE, Double.MAX_VALUE);
	private volatile TLongDoubleHashMap current = new TLongDoubleHashMap(8,
			.75f, Long.MAX_VALUE, Double.MAX_VALUE);
	private DoubleMessageMerger merger;

	public DoubleMessageQueue(DoubleMessageMerger merger) {
		this.merger = merger;
	}

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

	@Override
	public void flush(String msgType, String subgraph, WorkerTask workerTask)
			throws Exception {
		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			final TLongDoubleIterator it = readOnly.iterator();
			while (it.hasNext()) {
				it.advance();
				long to = it.key();
				if (to != -1) {
					Worker w = workerTask.getWorker(to);
					sizes.adjustOrPutValue(w, 1, 1);
				}
			}
		}

		HashMap<Worker, DoubleData> ret = new HashMap<>();
		final TLongDoubleIterator it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			long to = it.key();
			if (to == -1) {
				workerTask.outputDouble(msgType, -1l, -1l, it.value());
			} else {
				Worker w = workerTask.getWorker(to);
				DoubleData data = ret.get(w);
				if (data == null) {
					data = new DoubleData(sizes.get(w));
					ret.put(w, data);
				}
				data.addL(to);
				data.addF(it.value());
			}
		}
		readOnly.clear();
		workerTask.sendDoubles(msgType, ret);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		final double found = this.readOnly.get(to);
		if (found == this.readOnly.getNoEntryValue())
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new DoublePregelMessage(msgType, -1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

	@Override
	public long[] keys() {
		return readOnly.keys();
	}

}
