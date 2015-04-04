package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.jlime.pregel.worker.PersistentOrderedQueue.MessageID;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

final class PersistentQueueIterator implements Iterator<List<PregelMessage>> {
	private final Iterator<Entry<MessageID, PregelMessage>> backIt;
	private final TLongIterator cacheIt;

	Entry<MessageID, PregelMessage> currBack = null;
	List<PregelMessage> currCache = null;
	private TLongObjectHashMap<List<PregelMessage>> cache;

	PersistentQueueIterator(Iterator<Entry<MessageID, PregelMessage>> backIt,
			TLongArrayList list, TLongObjectHashMap<List<PregelMessage>> cache2) {
		this.backIt = backIt;
		this.cacheIt = list.iterator();
		this.cache = cache2;

	}

	@Override
	public List<PregelMessage> next() {
		if (currBack == null && backIt != null && backIt.hasNext())
			currBack = backIt.next();
		if (currCache == null && cacheIt != null && cacheIt.hasNext()) {
			currCache = cache.get(cacheIt.next());
		}

		List<PregelMessage> ret = null;
		if (currCache != null) {
			if (currBack != null) {
				int compare = Long.compare(currBack.getKey().vid, currCache
						.get(0).getTo());
				if (compare == 0) {
					ret = currCache;
					currCache = null;
					addBacked(backIt, ret);
				} else if (compare > 0) {
					ret = currCache;
					currCache = null;
				} else if (compare < 0) {
					ret = new ArrayList<>();
					addBacked(backIt, ret);
				}
			} else {
				ret = currCache;
				currCache = null;
			}
		} else {
			ret = new ArrayList<>();
			addBacked(backIt, ret);
		}
		return ret;
	}

	private void addBacked(
			final Iterator<Entry<MessageID, PregelMessage>> backIt,
			List<PregelMessage> ret) {
		long vertex = currBack.getKey().vid;
		while (vertex == currBack.getKey().vid) {
			ret.add(currBack.getValue());
			if (!backIt.hasNext()) {
				currBack = null;
				break;
			}
			currBack = backIt.next();
		}
	}

	@Override
	public boolean hasNext() {
		return currBack != null || currCache != null
				|| (backIt != null && backIt.hasNext())
				|| (cacheIt != null && cacheIt.hasNext());
	}
}