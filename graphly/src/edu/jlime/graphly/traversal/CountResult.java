package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.procedure.TLongIntProcedure;

public class CountResult implements TraversalResult {

	private TLongIntHashMap vals;

	public CountResult(TLongIntHashMap intvals) {
		this.vals = intvals;
	}

	@Override
	public TLongArrayList vertices() {
		return TLongArrayList.wrap(vals.keys());
	}

	@Override
	public int getInt(long k) {
		return vals.get(k);
	}

	@Override
	public TraversalResult removeAll(TLongArrayList v) {		
		vals.retainEntries(new TLongIntProcedure() {

			@Override
			public boolean execute(long k, int val) {
				return !v.contains(k);
			}
		});
		return new CountResult(vals);
	}

	@Override
	public TraversalResult retainAll(TLongArrayList v) {
		vals.retainEntries(new TLongIntProcedure() {

			@Override
			public boolean execute(long k, int val) {
				return v.contains(k);
			}
		});
		return new CountResult(vals);
	}

	
	@Override
	public String toString() {
		return vals.toString();
	}
}
