package edu.jlime.pregel.queues;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import gnu.trove.map.hash.TLongFloatHashMap;

public class FloatMessageQueue {
	// private Map<UUID, TLongFloatHashMap> current = new ConcurrentHashMap<>();

	TLongFloatHashMap current = new TLongFloatHashMap(8, .75f, NO_KEY,
			NO_VALUE);

	private static final float NO_VALUE = Float.MIN_VALUE;
	private static final long NO_KEY = Long.MIN_VALUE;

	private FloatTroveMessageMerger merger;
	private int currentsize = 0;

	public FloatMessageQueue(MessageMerger merger) {
		this.merger = (FloatTroveMessageMerger) merger;
	}

	public synchronized void putFloat(long from, long to, float msg) {
		// TLongFloatHashMap map = getMap(wID);
		// synchronized (map) {
		int old = current.size();
		merger.merge(to, msg, current);
		if (current.size() != old)
			currentsize++;
		// }
	}

	public int size() {
		return currentsize;
	}

	public Iterator<PregelMessage> getMessages(final String msgType,
			final long to) {
		List<PregelMessage> msgs = new ArrayList<>();
		// for (TLongFloatHashMap map : current.values())
		float get = current.get(to);
		if (get != current.getNoEntryValue())
			msgs.add(new FloatMessage(msgType, -1, to, get));
		return msgs.iterator();
	}

	public long[] keys() {
		// TLongArrayList ret = new TLongArrayList();
		// for (TLongFloatHashMap tLongFloatHashMap : current.values()) {
		// ret.addAll(tLongFloatHashMap.keys());
		// }
		return current.keys();
	}

	public synchronized void putFloat(long from, long[] to, float[] vals) {
		// TLongFloatHashMap map = getMap(wID);
		// synchronized (map) {
		for (int i = 0; i < vals.length; i++) {
			int old = current.size();
			merger.merge(to[i], vals[i], current);
			if (current.size() != old)
				currentsize++;
		}
		// }
	}

	// private TLongFloatHashMap getMap(UUID wID) {
	// TLongFloatHashMap map = current.get(wID);
	// if (map == null)
	// synchronized (this) {
	// map = current.get(wID);
	// if (map == null) {
	// map = new TLongFloatHashMap(8, .75f, NO_KEY, NO_VALUE);
	// current.put(wID, map);
	// }
	// }
	// return map;
	// }
}
