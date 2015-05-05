package edu.jlime.pregel.queues;

import java.util.Iterator;
import java.util.List;

import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;

public interface PregelMessageQueue {

	public abstract void switchQueue();

	public abstract int currentSize();

	public abstract int readOnlySize();

	public abstract Iterator<List<PregelMessage>> iterator();

	public abstract void put(long vid, long to, Object msg);

	public abstract void flush(WorkerTask workerTask) throws Exception;

	public abstract void putFloat(long from, long to, float val);

	public abstract void putDouble(long from, long to, double val);

	public abstract Iterator<PregelMessage> getMessages(long currentVertex);
}