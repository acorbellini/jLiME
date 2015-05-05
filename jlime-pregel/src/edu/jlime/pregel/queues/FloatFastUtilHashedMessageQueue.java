package edu.jlime.pregel.queues;

import gnu.trove.map.hash.TObjectIntHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.FloatMessageMerger;
import edu.jlime.pregel.worker.FloatSenderCallback;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;

public class FloatFastUtilHashedMessageQueue implements PregelMessageQueue {
	private volatile Long2FloatOpenHashMap readOnly = new Long2FloatOpenHashMap();
	private volatile Long2FloatOpenHashMap current = new Long2FloatOpenHashMap();
	private FloatMessageMerger merger;

	public FloatFastUtilHashedMessageQueue(FloatMessageMerger merger) {
		this.merger = merger;
		readOnly.defaultReturnValue(Float.MIN_VALUE);
		current.defaultReturnValue(Float.MIN_VALUE);
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
		if (found == current.defaultReturnValue()) {
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
		Long2FloatOpenHashMap aux = readOnly;
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
			final LongIterator it = readOnly.keySet().iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				Long cursor = it.nextLong();
				ret.add(new FloatPregelMessage(-1, cursor, readOnly.get(cursor)));
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
	public void flush(final WorkerTask workerTask) throws Exception {
		workerTask.sendFloats(new FloatSenderCallback() {
			@Override
			public HashMap<Worker, FloatData> buildMap() throws Exception {
				return toMap(workerTask);
			}

		});

	}

	private HashMap<Worker, FloatData> toMap(final WorkerTask workerTask)
			throws Exception {
		HashMap<Worker, FloatData> ret = new HashMap<Worker, FloatData>();
		TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		{
			final LongIterator it = readOnly.keySet().iterator();
			while (it.hasNext()) {
				long to = it.nextLong();
				if (to != -1) {
					Worker w = workerTask.getWorker(to);
					sizes.adjustOrPutValue(w, 1, 1);
				}
			}
		}

		final LongIterator it = readOnly.keySet().iterator();
		while (it.hasNext()) {
			long to = it.nextLong();
			if (to == -1) {
				workerTask.outputFloat(-1l, -1l, readOnly.get(to));
			} else {
				Worker w = workerTask.getWorker(to);
				FloatData data = ret.get(w);
				if (data == null) {
					data = new FloatData(sizes.get(w));
					ret.put(w, data);
				}
				data.addL(to);
				data.addF(readOnly.get(to));
			}
		}
		readOnly.clear();
		return ret;
	}

	@Override
	public void putDouble(long from, long to, double val) {
		putFloat(from, to, (float) val);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final long to) {
		final float found = this.readOnly.get(to);
		if (found == readOnly.defaultReturnValue())
			return null;
		else
			return new Iterator<PregelMessage>() {
				boolean first = true;

				@Override
				public PregelMessage next() {
					first = false;
					return new FloatPregelMessage(-1l, to, found);
				}

				@Override
				public boolean hasNext() {
					return first;
				}
			};
	}

}
