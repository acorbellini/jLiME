package edu.jlime.pregel.worker;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;

public class InMemVertexList implements VertexList {
	TLongArrayList list = new TLongArrayList();

	public InMemVertexList() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(long vid) throws Exception {
		list.add(vid);
	}

	@Override
	public LongIterator iterator() throws Exception {
		final TLongIterator it = list.iterator();
		return new LongIterator() {

			@Override
			public long next() {
				return it.next();
			}

			@Override
			public boolean hasNext() throws Exception {
				return it.hasNext();
			}
		};
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public void flush() throws Exception {
	}

	@Override
	public void delete() throws Exception {
	}

}
