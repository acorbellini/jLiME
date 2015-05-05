package edu.jlime.pregel.mergers;

import edu.jlime.pregel.queues.MessageQueueFactory;

public abstract class ObjectMessageMerger implements MessageMerger {
	public abstract Object merge(Object o1, Object o2);

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.hashed(this);
	}
}
