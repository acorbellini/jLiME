package edu.jlime.pregel.worker;


public abstract class FloatMessageMerger implements MessageMerger {

	public abstract float merge(float msg1, float msg2);

	@Override
	public MessageQueueFactory getFactory() {
		return MessageQueueFactory.floatQueue(this);
	}

}
