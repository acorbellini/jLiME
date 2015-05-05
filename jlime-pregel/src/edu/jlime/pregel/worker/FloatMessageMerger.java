package edu.jlime.pregel.worker;

import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.queues.MessageQueueFactory;


public abstract class FloatMessageMerger implements MessageMerger {

	public abstract float merge(float msg1, float msg2);

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.floatQueue(this);
	}

}
