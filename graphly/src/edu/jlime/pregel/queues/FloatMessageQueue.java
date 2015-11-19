package edu.jlime.pregel.queues;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatData;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.pregel.worker.rpc.Worker;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class FloatMessageQueue implements PregelMessageQueue {
	private Map<UUID, TLongFloatHashMap> current = new ConcurrentHashMap<>();

	private static final float NO_VALUE = Float.MIN_VALUE;
	private static final long NO_KEY = Long.MIN_VALUE;

	private FloatTroveMessageMerger merger;
	private int currentsize = 0;

	public FloatMessageQueue(FloatTroveMessageMerger merger) {
		this.merger = merger;
	}

	public void putFloat(UUID wID, long from, long to, float msg) {
		TLongFloatHashMap map = current.get(wID);
		if (map == null) {
			synchronized (this) {
				map = current.get(wID);
				if (map == null) {
					map = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
					current.put(wID, map);
				}
			}

		}
		synchronized (map) {
			int old = map.size();
			merger.merge(to, msg, map);
			if (map.size() != old)
				currentsize++;
		}
	}

	public int size() {
		return currentsize;
	}

	public void flush(String msgType, String subgraph,
			final WorkerTask workerTask) throws Exception {
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
						workerTask.outputFloatSubgraph(msgType, subgraph, -1l,
								it.value());
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

	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		List<PregelMessage> msgs = new ArrayList<>();
		for (TLongFloatHashMap map : current.values())
			if (map.containsKey(to))
				msgs.add(new FloatMessage(msgType, -1, to, map.get(to)));
		return msgs.iterator();
	}

	public long[] keys() {
		TLongArrayList ret = new TLongArrayList();
		for (TLongFloatHashMap tLongFloatHashMap : current.values()) {
			ret.addAll(tLongFloatHashMap.keys());
		}
		return ret.toArray();
	}

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

	public void putFloat(UUID wID, long from, long[] to, float[] vals) {
		TLongFloatHashMap map = current.get(wID);
		synchronized (map) {
			if (map == null) {
				map = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
				current.put(wID, map);
			}
			for (int i = 0; i < vals.length; i++) {
				int old = map.size();
				merger.merge(to[i], vals[i], map);
				if (map.size() != old)
					currentsize++;
			}
		}
	}
}
