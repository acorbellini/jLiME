package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;

import edu.jlime.pregel.messages.PregelMessage;

public class ConcatIterator implements Iterator<PregelMessage> {

	private List<Iterator<PregelMessage>> l;
	int cont = 0;
	Iterator<PregelMessage> curr = null;

	public ConcatIterator(List<Iterator<PregelMessage>> ret) {
		this.l = ret;
	}

	@Override
	public boolean hasNext() {
		while (curr == null || !curr.hasNext()) {
			if (l.isEmpty())
				return false;
			curr = l.remove(0);
		}
		return true;
	}

	@Override
	public PregelMessage next() {
		return curr.next();
	}

}
