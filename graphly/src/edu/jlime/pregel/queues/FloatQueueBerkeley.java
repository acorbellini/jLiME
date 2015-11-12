package edu.jlime.pregel.queues;

import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

public class FloatQueueBerkeley implements FloatMessageQueue {

	private static final int LOCKS = 2048;
	BerkeleyKV readOnly = new BerkeleyKV();
	BerkeleyKV current = new BerkeleyKV();
	private FloatMessageMerger merger;

	private Object[] locks = new Object[LOCKS];

	public FloatQueueBerkeley(FloatMessageMerger merger) {
		this.merger = merger;
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new Object();
		}
	}

	@Override
	public int size() {
		return (int) readOnly.size();
	}

	@Override
	public void flush(String msgType, String subgraph, WorkerTask workerTask)
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
		final Float found = this.readOnly.get(to);
		if (found == null)
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
	public void putFloat(UUID wid, long from, long to, float msg) {
		Object lock = locks[(int) Math.abs(to % LOCKS)];
		synchronized (lock) {
			Float found = this.current.get(to);
			if (found == null) {
				this.current.put(to, msg);
			} else {
				this.current.put(to, merger.merge(found, msg));
			}
		}
	}

	@Override
	public long[] keys() {
		return readOnly.keys();
	}

	@Override
	public void transferTo(PregelMessageQueue cache) {

	}

}
