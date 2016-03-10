package edu.jlime.graphly.rec;

import edu.jlime.pregel.coordinator.Aggregator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class IntersectAggregator implements SetAggregator {
	TLongHashSet set = null;

	@Override
	public void superstep(int s) {
	}

	@Override
	public Aggregator copy() {
		return new IntersectAggregator();
	}

	@Override
	public void reset() {
	}

	@Override
	public void merge(Aggregator value) {
		if (set != null)
			add(((IntersectAggregator) value).set);
	}

	public void add(TLongHashSet out) {
		if (set == null)
			set = new TLongHashSet(out);
		else {
			TLongIterator it = set.iterator();
			while (it.hasNext()) {
				if (!out.contains(it.next()))
					it.remove();
			}
		}
	}

	public TLongHashSet getSet() {
		if (set == null)
			return new TLongHashSet();
		return set;
	}

	@Override
	public float get() {
		return set != null ? set.size() : 0f;
	}
}
