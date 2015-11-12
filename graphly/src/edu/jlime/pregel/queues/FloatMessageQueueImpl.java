package edu.jlime.pregel.queues;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

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
	private HashMap<UUID, TLongFloatHashMap> current = new HashMap<>();

	private static final float NO_VALUE = Float.MIN_VALUE;
	private static final long NO_KEY = Long.MIN_VALUE;

	private FloatTroveMessageMerger merger;
	private int currentsize = 0;

	public FloatMessageQueueImpl(FloatTroveMessageMerger merger) {
		this.merger = merger;
	}

	public void putFloat(UUID wID, long from, long to, float msg) {
		TLongFloatHashMap map = current.get(wID);
		if (map == null) {
			map = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
			current.put(wID, map);
		}
		int old = map.size();
		merger.merge(to, msg, map);
		if (map.size() != old)
			currentsize++;
	}

	@Override
	public int size() {
		return currentsize;
	}

	@Override
	public void flush(String msgType, String subgraph, final WorkerTask workerTask) throws Exception {

		// TObjectIntHashMap<Worker> sizes = new TObjectIntHashMap<>();
		// {
		// for (TLongFloatHashMap map : current.values()) {
		// final TLongFloatIterator it = map.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// long to = it.key();
		// if (to != -1) {
		// Worker w = workerTask.getWorker(to);
		// sizes.adjustOrPutValue(w, 1, 1);
		// }
		// }
		// }
		// }
		//
		// HashMap<Worker, FloatData> ret = new HashMap<Worker, FloatData>();
		// for (TLongFloatHashMap map : current.values()) {
		// final TLongFloatIterator it = map.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// long to = it.key();
		// if (to == -1) {
		// if (subgraph == null)
		// workerTask.outputFloat(msgType, -1l, -1l, it.value());
		// else
		// workerTask.outputFloatSubgraph(msgType, subgraph, -1l, it.value());
		// } else {
		// Worker w = workerTask.getWorker(to);
		// FloatData data = ret.get(w);
		// if (data == null) {
		// data = new FloatData(sizes.get(w));
		// ret.put(w, data);
		// }
		// data.addL(to);
		// data.addF(it.value());
		// }
		// }
		// map.clear();
		// }
		HashMap<Worker, FloatData> ret = new HashMap<Worker, FloatData>();
		for (Entry<UUID, TLongFloatHashMap> e : current.entrySet()) {
			Worker workerByID = workerTask.getWorkerByID(e.getKey());
			TLongFloatHashMap map = e.getValue();
			if (workerByID == null) {
				// TODO Improve Broadcast sending. Add proper APIs for batch
				// send.
				TLongFloatIterator it = map.iterator();
				while (it.hasNext()) {
					it.advance();
					if (subgraph == null)
						workerTask.outputFloat(msgType, -1l, -1l, it.value());
					else
						workerTask.outputFloatSubgraph(msgType, subgraph, -1l, it.value());
				}
			} else {
				FloatData fData = new FloatData(map.keys(), map.values());

				ret.put(workerByID, fData);
			}

			map.clear();
		}

		current.clear();

		workerTask.sendFloats(msgType, ret);

	}

	@Override
	public Iterator<PregelMessage> getMessages(final String msgType, final long to) {
		List<PregelMessage> msgs = new ArrayList<>();
		for (TLongFloatHashMap map : current.values())
			if (map.containsKey(to))
				msgs.add(new FloatMessage(msgType, -1, to, map.get(to)));
		return msgs.iterator();
	}

	@Override
	public long[] keys() {
		TLongArrayList ret = new TLongArrayList();
		for (TLongFloatHashMap tLongFloatHashMap : current.values()) {
			ret.addAll(tLongFloatHashMap.keys());
		}
		return ret.toArray();
	}

	@Override
	public void transferTo(PregelMessageQueue cache) {
		FloatMessageQueue q = (FloatMessageQueue) cache;
		for (Entry<UUID, TLongFloatHashMap> e : current.entrySet()) {
			TLongFloatIterator it = e.getValue().iterator();
			while (it.hasNext()) {
				it.advance();
				q.putFloat(e.getKey(), -1, it.key(), it.value());
			}
			e.getValue().clear();
		}
	}

}
