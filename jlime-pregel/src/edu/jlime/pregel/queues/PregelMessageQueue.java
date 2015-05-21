package edu.jlime.pregel.queues;

import java.util.Iterator;

import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;

public interface PregelMessageQueue {

	public abstract void switchQueue();

	public abstract int currentSize();

	public abstract int readOnlySize();

	public abstract void flush(String msgType, WorkerTask workerTask)
			throws Exception;

	public abstract Iterator<PregelMessage> getMessages(String msgType,
			long currentVertex);
	// public abstract Iterator<List<PregelMessage>> iterator();

	// public abstract void put(long vid, long to, Object msg) throws Exception;

	// public abstract void putFloat(long from, long to, float val)
	// throws Exception;
	//
	// public abstract void putDouble(long from, long to, double val)
	// throws Exception;

}