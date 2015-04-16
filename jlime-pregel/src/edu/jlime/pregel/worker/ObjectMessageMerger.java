package edu.jlime.pregel.worker;

public abstract class ObjectMessageMerger implements MessageMerger {
	public abstract Object merge(Object o1, Object o2);

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.hashed(this);
	}
}
