package edu.jlime.pregel.queues;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import edu.jlime.pregel.mergers.MessageMergers.FloatArrayMerger;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

public class FloatArrayMessageQueue implements PregelMessageQueue {

	public static final float NULL = Float.MIN_VALUE;

	private FloatArrayMerger merger;

	private TLongObjectHashMap<float[]> current = new TLongObjectHashMap<float[]>();

	private TLongObjectHashMap<float[]> readOnly = new TLongObjectHashMap<float[]>();

	public FloatArrayMessageQueue(FloatArrayMerger messageMerger) {
		this.merger = messageMerger;
	}

	@Override
	public void switchQueue() {
		TLongObjectHashMap<float[]> aux = current;
		current = readOnly;
		readOnly = aux;
		current.clear();
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
	public void flush(String msgType, String subgraph, WorkerTask workerTask)
			throws Exception {
		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			final TLongObjectIterator<float[]> it = readOnly.iterator();
			while (it.hasNext()) {
				it.advance();
				long to = it.key();
				if (to != -1) {
					Worker w = workerTask.getWorker(to);
					sizes.adjustOrPutValue(w, 1, 1);
				}
			}
		}

		HashMap<Worker, FloatArrayData> ret = new HashMap<>();
		final TLongObjectIterator<float[]> it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			long to = it.key();
			if (to == -1) {
				workerTask.outputFloatArray(msgType, -1l, -1l, it.value());
			} else {
				Worker w = workerTask.getWorker(to);
				FloatArrayData data = ret.get(w);
				if (data == null) {
					data = new FloatArrayData(sizes.get(w));
					ret.put(w, data);
				}
				data.addL(to);
				data.addF(it.value());
			}
		}
		readOnly.clear();
		workerTask.sendFloatArrays(msgType, ret);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		final float[] found = this.readOnly.get(to);
		if (found == null)
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new FloatArrayPregelMessage(msgType, -1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

	public synchronized void putFloatArray(long from, long to, float[] val) {
		float[] found = current.get(to);
		if (found == null)
			current.put(to, Arrays.copyOf(val, val.length));
		else
			merger.merge(val, found, found);

	}

	@Override
	public long[] keys() {
		return readOnly.keys();
	}
}
