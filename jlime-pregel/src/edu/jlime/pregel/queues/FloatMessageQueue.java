package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.FloatMessageMerger;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.HashMap;
import java.util.Iterator;

public class FloatMessageQueue implements PregelMessageQueue {

	private volatile TLongFloatHashMap readOnly = new TLongFloatHashMap(8,
			.75f, Long.MAX_VALUE, Float.MAX_VALUE);
	private volatile TLongFloatHashMap current = new TLongFloatHashMap(8, .75f,
			Long.MAX_VALUE, Float.MAX_VALUE);
	private FloatMessageMerger merger;

	public FloatMessageQueue(FloatMessageMerger merger) {
		this.merger = merger;
	}

	public synchronized void putFloat(long from, long to, float msg) {
		float found = this.current.get(to);
		if (found == this.current.getNoEntryValue()) {
			this.current.put(to, msg);
		} else {
			this.current.put(to, merger.merge(found, msg));
		}
	}

	@Override
	public synchronized void switchQueue() {
		TLongFloatHashMap aux = readOnly;
		this.readOnly = current;
		this.current = aux;
		this.current.clear();
	}

	@Override
	public int currentSize() {
		return current.size();
	}

	@Override
	public int readOnlySize() {
		return readOnly.size();
	}

	@Override
	public void flush(String msgType, final WorkerTask workerTask)
			throws Exception {

		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			final TLongFloatIterator it = readOnly.iterator();
			while (it.hasNext()) {
				it.advance();
				long to = it.key();
				if (to != -1) {
					Worker w = workerTask.getWorker(to);
					sizes.adjustOrPutValue(w, 1, 1);
				}
			}
		}

		HashMap<Worker, FloatData> ret = new HashMap<Worker, FloatData>();
		final TLongFloatIterator it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			long to = it.key();
			if (to == -1) {
				workerTask.outputFloat(msgType, -1l, -1l, it.value());
			} else {
				Worker w = workerTask.getWorker(to);
				FloatData data = ret.get(w);
				if (data == null) {
					data = new FloatData(sizes.get(w));
					ret.put(w, data);
				}
				data.addL(to);
				data.addF(it.value());
			}
		}
		readOnly.clear();
		workerTask.sendFloats(msgType, ret);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		final float found = this.readOnly.get(to);
		if (found == this.readOnly.getNoEntryValue())
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new FloatPregelMessage(msgType, -1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

}
