package edu.jlime.pregel.worker;

import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.queues.MessageQueueFactory;
import gnu.trove.map.hash.TLongFloatHashMap;

public abstract class FloatTroveMessageMerger implements MessageMerger {

	public abstract void merge(long to, float msg2, TLongFloatHashMap map);

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.floatQueue(this);
	}

}
