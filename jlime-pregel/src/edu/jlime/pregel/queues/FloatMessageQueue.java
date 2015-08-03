package edu.jlime.pregel.queues;

public interface FloatMessageQueue extends PregelMessageQueue {
	public void putFloat(long from, long to, float msg);
}
