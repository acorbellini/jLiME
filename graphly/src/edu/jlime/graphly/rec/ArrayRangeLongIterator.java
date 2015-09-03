package edu.jlime.graphly.rec;

import edu.jlime.pregel.worker.LongIterator;

public class ArrayRangeLongIterator implements LongIterator {

	private long[] data;
	private int to;
	private int from = 0;

	public ArrayRangeLongIterator(long[] data, int from, int to) {
		this.data = data;
		this.from = from;
		this.to = to;
	}

	@Override
	public boolean hasNext() throws Exception {
		return from < to;
	}

	@Override
	public long next() {
		return data[from++];
	}

}
