package edu.jlime.pregel.worker;

public interface LongIterator {
	public boolean hasNext() throws Exception;

	public long next();
}
