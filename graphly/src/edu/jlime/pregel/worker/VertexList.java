package edu.jlime.pregel.worker;

public interface VertexList {
	public void add(long vid) throws Exception;

	public LongIterator iterator() throws Exception;

	public int size();

	public void flush() throws Exception;

	public void close() throws Exception;

	public void delete() throws Exception;
}
