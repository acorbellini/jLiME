package edu.jlime.pregel.queues;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.mapdb.BTreeMap;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;

import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.list.array.TLongArrayList;

final class PersistentQueueIterator implements Iterator<List<PregelMessage>> {
	private final Iterator<Entry<Fun.Tuple2<Long, Long>, PregelMessage>> backIt;
	private final Iterator<Entry<Tuple2<Long, Long>, PregelMessage>> cacheIt;

	Entry<Fun.Tuple2<Long, Long>, PregelMessage> currBack = null;
	Entry<Tuple2<Long, Long>, PregelMessage> currCache = null;
	private BTreeMap<Tuple2<Long, Long>, PregelMessage> cache;

	PersistentQueueIterator(
			Iterator<Entry<Fun.Tuple2<Long, Long>, PregelMessage>> backIt,
			TLongArrayList list,
			BTreeMap<Tuple2<Long, Long>, PregelMessage> cache2) {
		this.backIt = backIt;
		this.cacheIt = cache2.entrySet().iterator();

	}

	@Override
	public List<PregelMessage> next() {
		if (currBack == null && backIt != null && backIt.hasNext())
			currBack = backIt.next();
		if (currCache == null && cacheIt != null && cacheIt.hasNext()) {
			currCache = cacheIt.next();
		}

		List<PregelMessage> ret = new ArrayList<>();
		if (currCache != null) {
			if (currBack != null) {
				// Comparing using "To"
				int compare = Long.compare(currBack.getKey().b,
						currBack.getKey().b);
				if (compare == 0) {
					addCache(ret);
					addBacked(ret);
				} else if (compare > 0) {
					addCache(ret);
				} else if (compare < 0) {
					addBacked(ret);
				}
			} else {
				addCache(ret);
			}
		} else {
			addBacked(ret);
		}
		return ret;
	}

	private void addCache(List<PregelMessage> ret) {
		long vertex = currCache.getKey().b;
		while (vertex == currCache.getKey().b) {
			ret.add(currCache.getValue());
			if (!cacheIt.hasNext()) {
				currCache = null;
				break;
			}
			currCache = cacheIt.next();
		}
	}

	private void addBacked(List<PregelMessage> ret) {
		long vertex = currBack.getKey().b;
		while (vertex == currBack.getKey().b) {
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