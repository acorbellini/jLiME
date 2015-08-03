package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.HashMap;
import java.util.Iterator;

public class FloatMessageQueueImpl implements FloatMessageQueue {

	private static final int SIZE = 16;
	private volatile TLongFloatHashMap[] readOnly = new TLongFloatHashMap[16];
	private volatile TLongFloatHashMap[] current = new TLongFloatHashMap[16];
	private FloatTroveMessageMerger merger;

	public FloatMessageQueueImpl(FloatTroveMessageMerger merger) {
		for (int i = 0; i < current.length; i++) {
			current[i] = new TLongFloatHashMap(8, .75f, Long.MAX_VALUE,
					Float.MAX_VALUE);
			readOnly[i] = new TLongFloatHashMap(8, .75f, Long.MAX_VALUE,
					Float.MAX_VALUE);
		}
		this.merger = merger;
	}

	private int getHash(long to) {
		int hash = Math.abs((int) ((to * 2147483647l) % SIZE));
		return hash;
	}

	public void putFloat(long from, long to, float msg) {
		TLongFloatHashMap map = current[getHash(to)];
		synchronized (map) {
			merger.merge(to, msg, map);
		}
	}

	@Override
	public synchronized void switchQueue() {
		// readOnly.clear();
		// TLongFloatIterator it = current.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// readOnly.put(it.key(), it.value());
		// }
		for (int i = 0; i < current.length; i++) {
			TLongFloatHashMap aux = readOnly[i];
			readOnly[i] = current[i];
			current[i] = aux;
			this.current[i].clear();
		}

	}

	@Override
	public int currentSize() {
		int size = 0;
		for (int i = 0; i < current.length; i++) {
			size += current[i].size();
		}
		return size;
	}

	@Override
	public int readOnlySize() {
		int size = 0;
		for (int i = 0; i < readOnly.length; i++) {
			size += readOnly[i].size();
		}
		return size;
	}

	@Override
	public void flush(String msgType, final WorkerTask workerTask)
			throws Exception {

		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			for (int i = 0; i < readOnly.length; i++) {
				final TLongFloatIterator it = readOnly[i].iterator();
				while (it.hasNext()) {
					it.advance();
					long to = it.key();
					if (to != -1) {
						Worker w = workerTask.getWorker(to);
						sizes.adjustOrPutValue(w, 1, 1);
					}
				}
			}
		}

		HashMap<Worker, FloatData> ret = new HashMap<Worker, FloatData>();
		for (int i = 0; i < readOnly.length; i++) {
			final TLongFloatIterator it = readOnly[i].iterator();
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
			readOnly[i].clear();
		}
		workerTask.sendFloats(msgType, ret);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		int hash = getHash(to);
		final float found = this.readOnly[hash].get(to);
		if (found == this.readOnly[hash].getNoEntryValue())
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
