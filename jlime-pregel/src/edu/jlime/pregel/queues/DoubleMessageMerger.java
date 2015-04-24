package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.MessageMerger;

public abstract class DoubleMessageMerger implements MessageMerger {

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.doubleQueue(this);

	}

	public abstract double merge(double found, double msg);

}
