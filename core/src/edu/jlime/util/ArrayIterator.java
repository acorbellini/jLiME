package edu.jlime.util;

import java.util.Iterator;

public class ArrayIterator<R> implements Iterator<R> {

	private R[] array;

	private int pos = 0;

	public ArrayIterator(R[] array) {
		this.array = array;
	}

	@Override
	public boolean hasNext() {
		return (pos != array.length);
	}

	public R peek() {
		return array[pos];
	}

	@Override
	public R next() {
		return array[pos++];
	}

	@Override
	public void remove() {

	}

}
