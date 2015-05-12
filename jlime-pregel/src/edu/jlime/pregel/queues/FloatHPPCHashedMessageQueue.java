package edu.jlime.pregel.queues;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.carrotsearch.hppc.LongFloatOpenHashMap;
import com.carrotsearch.hppc.cursors.LongFloatCursor;

import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatMessageMerger;
import edu.jlime.pregel.worker.WorkerTask;

public class FloatHPPCHashedMessageQueue implements PregelMessageQueue {
	private volatile LongFloatOpenHashMap readOnly = new LongFloatOpenHashMap(
			16, .75f);
	private volatile LongFloatOpenHashMap current = new LongFloatOpenHashMap(
			16, .75f);
	private FloatMessageMerger merger;

	public FloatHPPCHashedMessageQueue(FloatMessageMerger merger) {
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
		float found = this.current.getOrDefault(to, Float.MIN_VALUE);
		if (found == Float.MIN_VALUE) {
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
		LongFloatOpenHashMap aux = readOnly;
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
			final Iterator<LongFloatCursor> it = readOnly.iterator();

			@Override
			public List<PregelMessage> next() {
				ArrayList<PregelMessage> ret = new ArrayList<PregelMessage>();
				LongFloatCursor cursor = it.next();
				ret.add(new FloatPregelMessage(-1, cursor.key, cursor.value));
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
		// TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		// HashMap<Worker, TLongArrayList> keys = new HashMap<>();
		// HashMap<Worker, TFloatArrayList> values = new HashMap<>();
		//
		// {
		// final Iterator<LongFloatCursor> it = readOnly.iterator();
		// while (it.hasNext()) {
		// LongFloatCursor cursor = it.next();
		//
		// long to = cursor.key;
		// if (to != -1) {
		// Worker w = workerTask.getWorker(to);
		// sizes.adjustOrPutValue(w, 1, 1);
		// }
		// }
		// }
		//
		// final Iterator<LongFloatCursor> it = readOnly.iterator();
		// while (it.hasNext()) {
		// LongFloatCursor cursor = it.next();
		// long to = cursor.key;
		// if (to == -1) {
		// workerTask.outputFloat(-1l, -1l, cursor.value);
		// } else {
		// Worker w = workerTask.getWorker(to);
		// TLongArrayList keyList = keys.get(w);
		// if (keyList == null) {
		// keyList = new TLongArrayList(sizes.get(w));
		// keys.put(w, keyList);
		// }
		// keyList.add(to);
		//
		// TFloatArrayList valList = values.get(w);
		// if (valList == null) {
		// valList = new TFloatArrayList();
		// values.put(w, valList);
		// }
		// valList.add(cursor.value);
		// }
		// }
		//
		// workerTask.sendFloats(keys, values);

	}

	@Override
	public void putDouble(long from, long to, double val) {
		putFloat(from, to, (float) val);
	}

	@Override
	public Iterator<PregelMessage> getMessages(final long to) {
		final float found = this.readOnly.getOrDefault(to, Float.MIN_VALUE);
		if (found == Float.MIN_VALUE)
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
