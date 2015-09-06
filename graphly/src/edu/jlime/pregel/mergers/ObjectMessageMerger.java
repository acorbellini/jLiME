package edu.jlime.pregel.mergers;

import edu.jlime.pregel.queues.MessageQueueFactory;

public abstract class ObjectMessageMerger<T> implements MessageMerger {

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.hashed(this);
	}

	public abstract T getCopy(T msg);

	public abstract void merge(T from, T into);
}
