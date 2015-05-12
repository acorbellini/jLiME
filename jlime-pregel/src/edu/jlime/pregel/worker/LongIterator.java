package edu.jlime.pregel.worker;

import java.io.IOException;

public interface LongIterator {
	public boolean hasNext() throws Exception;

	public long next();
}
