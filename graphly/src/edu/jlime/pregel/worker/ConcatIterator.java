package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Iterator;

import edu.jlime.pregel.messages.PregelMessage;

public class ConcatIterator implements Iterator<PregelMessage> {

	private Iterator<Iterator<PregelMessage>> l;
	Iterator<PregelMessage> curr = null;

	public ConcatIterator(ArrayList<Iterator<PregelMessage>> currList) {
		this.l = currList.iterator();
	}

	@Override
	public boolean hasNext() {
		while (curr == null || !curr.hasNext()) {
			if (!l.hasNext())
				return false;
			curr = l.next();
		}
		return true;
	}

	@Override
	public PregelMessage next() {
		return curr.next();
	}

}
