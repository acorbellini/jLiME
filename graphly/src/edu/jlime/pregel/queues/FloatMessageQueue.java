package edu.jlime.pregel.queues;

import java.util.UUID;

public interface FloatMessageQueue extends PregelMessageQueue {
	public void putFloat(UUID workerID, long from, long to, float msg);
}
