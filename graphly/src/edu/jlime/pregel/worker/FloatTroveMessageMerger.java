package edu.jlime.pregel.worker;

import edu.jlime.pregel.mergers.MessageMerger;
import gnu.trove.map.hash.TLongFloatHashMap;

public abstract class FloatTroveMessageMerger implements MessageMerger {

	public abstract void merge(long to, float msg2, TLongFloatHashMap map);
}
