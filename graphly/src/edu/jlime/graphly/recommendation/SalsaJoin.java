package edu.jlime.graphly.recommendation;

import java.util.Map;

import edu.jlime.graphly.traversal.Join;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

public class SalsaJoin implements Join<Map<Long, Float>, Float> {
	@Override
	public TLongObjectHashMap<Float> join(
			TLongObjectHashMap<Map<Long, Float>> input) {
		TLongObjectHashMap<Float> res = new TLongObjectHashMap<>();
		for (Long vid : input.keys()) {
			float sum = 0f;
			TLongObjectIterator<Map<Long, Float>> it = input.iterator();
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
