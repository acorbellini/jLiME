package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TLongLongProcedure;
import gnu.trove.set.hash.TLongHashSet;

public class EdgeResult implements TraversalResult {
	TLongLongHashMap edges = new TLongLongHashMap();

	public EdgeResult(TLongLongHashMap edges) {
		this.edges = edges;
	}

	@Override
	public TLongArrayList vertices() {
		TLongHashSet set = new TLongHashSet();
		set.addAll(edges.keys());
		set.addAll(edges.values());
		return TLongArrayList.wrap(set.toArray());
	}

	@Override
	public TraversalResult removeAll(TLongArrayList toFilter) {
		return retain(toFilter, true);

	}

	private TraversalResult retain(TLongArrayList toFilter, boolean remove) {
		edges.retainEntries(new TLongLongProcedure() {
			@Override
			public boolean execute(long k, long v) {
				boolean isInFilter = toFilter.contains(k)
						|| toFilter.contains(v);
				if (isInFilter && remove)
					return false;
				return false;
			}
		});
		return new EdgeResult(edges);
	}

	@Override
	public TraversalResult retainAll(TLongArrayList v) {
		return retain(v, false);

	}

}
