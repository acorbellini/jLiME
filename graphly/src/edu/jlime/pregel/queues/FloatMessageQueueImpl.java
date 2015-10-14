package edu.jlime.pregel.queues;

import java.util.HashMap;
import java.util.Iterator;

import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

public class FloatMessageQueueImpl implements FloatMessageQueue {

	private static final int HASHES = Runtime.getRuntime().availableProcessors();
	private static final float NO_VALUE = Float.MIN_VALUE;
	private static final long NO_KEY = Long.MIN_VALUE;

	private volatile TLongFloatHashMap[] readOnly = new TLongFloatHashMap[HASHES];
	private volatile TLongFloatHashMap[] current = new TLongFloatHashMap[HASHES];

	private FloatTroveMessageMerger merger;
	private int currentsize = 0;

	public FloatMessageQueueImpl(FloatTroveMessageMerger merger) {
		for (int i = 0; i < current.length; i++) {
			current[i] = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
			readOnly[i] = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
		}
		this.merger = merger;
	}

	private int getHash(long to) {
		int hash = Math.abs((int) ((to * 2147483647l) % HASHES));
		return hash;
	}

	public void putFloat(long from, long to, float msg) {
		TLongFloatHashMap map = current[getHash(to)];
		synchronized (map) {
			int old = map.size();
			merger.merge(to, msg, map);
			if (map.size() != old)
				currentsize++;
		}
	}

	@Override
	public synchronized void switchQueue() {
		for (int i = 0; i < current.length; i++) {
			TLongFloatHashMap aux = readOnly[i];
			readOnly[i] = current[i];
			current[i] = aux;
			this.current[i].clear();
			currentsize = 0;
		}

	}

	@Override
	public int currentSize() {
		return currentsize;
		// int size = 0;
		// for (int i = 0; i < current.length; i++) {
		// size += current[i].size();
		// }
		// return size;
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
	public void flush(String msgType, String subgraph, final WorkerTask workerTask) throws Exception {

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
					if (subgraph == null)
						workerTask.outputFloat(msgType, -1l, -1l, it.value());
					else
						workerTask.outputFloatSubgraph(msgType, subgraph, -1l, it.value());
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
	public Iterator<PregelMessage> getMessages(final String msgType, final long to) {
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
					return new FloatMessage(msgType, -1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

	@Override
	public long[] keys() {
		TLongArrayList ret = new TLongArrayList();
		for (TLongFloatHashMap tLongFloatHashMap : readOnly) {
			ret.addAll(tLongFloatHashMap.keys());
		}
		return ret.toArray();
	}

}
