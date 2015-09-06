package edu.jlime.pregel.worker;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ConcatList<T> extends AbstractList<T> {

	private List<T> l1;
	private List<T> l2;

	public ConcatList(List<T> l1, List<T> l2) {
		this.l1 = l1;
		this.l2 = l2;
	}

	@Override
	public int size() {
		return l1.size() + l2.size();
	}

	@Override
	public boolean isEmpty() {
		return l1.isEmpty() && l2.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return l1.contains(o) || l2.contains(o);
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < size();
			}

			@Override
			public T next() {
				return get(i++);
			}

		};
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object object : c) {
			if (!contains(object))
				return false;
		}
		return true;
	}

	@Override
	public T get(int index) {
		if (index < l1.size())
			return l1.get(index);
		else
			return l2.get(index - l1.size());
	}

	@Override
	public int indexOf(Object o) {
		int index = l1.indexOf(o);
		if (index < 0)
			index = l2.indexOf(o);
		return index;
	}

	@Override
	public int lastIndexOf(Object o) {
		int index = l1.lastIndexOf(o);
		if (index < 0)
			index = l2.lastIndexOf(o);
		return index;
	}

}