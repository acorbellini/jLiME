package edu.jlime.pregel.mergers;

import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

public class MessageMergers {
	public static FloatTroveMessageMerger floatSum() {
		return new FloatTroveMessageMerger() {
			@Override
			public void merge(long to, float msg2, TLongFloatHashMap map) {
				map.adjustOrPutValue(to, msg2, msg2);
			}

			@Override
			public <T> void merge(T k, float val,
					TObjectFloatHashMap<T> broadcast) {
				broadcast.adjustOrPutValue(k, val, val);
			}
		};
	}

}
