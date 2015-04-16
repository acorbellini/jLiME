package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;

public interface PregelMessageQueue {

	public abstract void switchQueue();

	public abstract int currentSize();

	public abstract int readOnlySize();

	public abstract Iterator<List<PregelMessage>> iterator();

	public abstract void put(long vid, long to, Object msg);

	public abstract void putFloat(long from, long to, float val);

	public abstract void flush(WorkerTask workerTask) throws Exception;

}