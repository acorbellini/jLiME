package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;

public class SegmentedIterator implements Iterator<List<PregelMessage>> {

	private SegmentedMessageQueue sq;

	int segment = 0;

	Iterator<List<PregelMessage>> currIt = null;

	public SegmentedIterator(SegmentedMessageQueue sq) {
		this.sq = sq;
	}

	@Override
	public boolean hasNext() {
		if (currIt != null && currIt.hasNext())
			return true;

		currIt = null;

		while (currIt == null && segment < sq.queue.length) {
			currIt = sq.queue[segment].iterator();
			if (currIt.hasNext())
				return true;
			currIt = null;
			segment++;
		}

		return false;
	}

	@Override
	public List<PregelMessage> next() {
		return currIt.next();
	}

}
