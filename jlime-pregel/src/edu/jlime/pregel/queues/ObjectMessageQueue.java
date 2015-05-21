package edu.jlime.pregel.queues;

public interface ObjectMessageQueue extends PregelMessageQueue {

	void put(long from, long to, Object msg);

}
