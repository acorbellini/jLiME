package edu.jlime.graphly.traversal;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TLongLongProcedure;
import gnu.trove.set.hash.TLongHashSet;

public class EdgeResult extends TraversalResult {
	TLongLongHashMap edges = new TLongLongHashMap();

	public EdgeResult(TLongLongHashMap edges) {
		this.edges = edges;
	}

	@Override
	public TLongHashSet vertices() {
		TLongHashSet set = new TLongHashSet();
		set.addAll(edges.keys());
		set.addAll(edges.values());
		return set;
	}

	@Override
	public TraversalResult removeAll(TLongHashSet toFilter) {
		return retain(toFilter, true);

	}

	private TraversalResult retain(final TLongHashSet toFilter, final boolean remove) {
		edges.retainEntries(new TLongLongProcedure() {
			@Override
			public boolean execute(long k, long v) {
				boolean isInFilter = toFilter.contains(k) || toFilter.contains(v);
				if (isInFilter && remove)
					return false;
				return false;
			}
		});
		return new EdgeResult(edges);
	}

	@Override
	public TraversalResult retainAll(TLongHashSet v) {
		return retain(v, false);

	}

	@Override
	public float getCount(long key) {
		return 0;
	}

	@Override
	public TLongFloatHashMap getCounts() {
		return new TLongFloatHashMap();
	}

}
