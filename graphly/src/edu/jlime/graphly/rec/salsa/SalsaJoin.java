package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.traversal.Join;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class SalsaJoin implements Join<TLongFloatHashMap, Float> {
	@Override
	public TLongObjectHashMap<Float> join(TLongObjectHashMap<TLongFloatHashMap> input) {
		TLongObjectHashMap<Float> res = new TLongObjectHashMap<>();
		for (Long vid : input.keys()) {
			float sum = 0f;
			TLongObjectIterator<TLongFloatHashMap> it = input.iterator();
			while (it.hasNext()) {
				it.advance();
				Float authval = it.value().get(vid);
				if (authval == null)
					authval = 0f;
				sum += authval;
			}
			res.put(vid, sum / input.size());
		}
		return res;

	}
}
