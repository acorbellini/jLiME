package edu.jlime.pregel.queues;

import java.util.HashMap;
import java.util.Iterator;

import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

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
	public void flush(String msgType, String subgraph, WorkerTask workerTask)
			throws Exception {
		// final TLongObjectIterator<Object> it = readOnly.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// workerTask.outputObject(msgType, -1, it.key(), it.value());
		// it.remove();
		// }

		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			final TLongObjectIterator<Object> it = readOnly.iterator();
			while (it.hasNext()) {
				it.advance();
				long to = it.key();
				if (to != -1) {
					Worker w = workerTask.getWorker(to);
					sizes.adjustOrPutValue(w, 1, 1);
				}
			}
		}

		HashMap<Worker, ObjectData> ret = new HashMap<>();
		final TLongObjectIterator<Object> it = readOnly.iterator();
		while (it.hasNext()) {
			it.advance();
			long to = it.key();
			if (to == -1) {
				if (subgraph == null)
					workerTask.outputObject(msgType, -1l, -1l, it.value());
				else
					workerTask.outputObjectSubgraph(msgType, subgraph, -1l,
							it.value());
			} else {
				Worker w = workerTask.getWorker(to);
				ObjectData data = ret.get(w);
				if (data == null) {
					data = new ObjectData(sizes.get(w));
					ret.put(w, data);
				}
				data.addL(to);
				data.addObj(it.value());
			}
		}
		readOnly.clear();
		workerTask.sendObjects(msgType, ret);

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

	@Override
	public long[] keys() {
		return readOnly.keys();
	}
}
