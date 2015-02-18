package edu.jlime.graphly.rec.randomwalk;

import java.util.List;

import edu.jlime.graphly.traversal.Join;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class RandomWalkJoin implements Join<List<long[]>, TLongFloatHashMap> {

	private TLongFloatHashMap normalize(List<long[]> input) {
		TLongFloatHashMap res = new TLongFloatHashMap();
		int cont = 0;
		for (long[] ls : input) {
			for (long l : ls) {
				res.adjustOrPutValue(l, 1f, 1f);
				cont++;
			}
		}

		long[] keys = res.keys();
		for (long l : keys) {
			res.put(l, res.get(l) / cont);
		}
		return res;
	}

	@Override
	public TLongObjectHashMap<TLongFloatHashMap> join(
			TLongObjectHashMap<List<long[]>> before) {
		TLongObjectHashMap<TLongFloatHashMap> finalRes = new TLongObjectHashMap<>();
		TLongObjectIterator<List<long[]>> it = before.iterator();
		while (it.hasNext()) {
			it.advance();
			long vid = it.key();
			List<long[]> input = it.value();
			TLongFloatHashMap tLongFloatMapDecorator = normalize(input);
			finalRes.put(vid, tLongFloatMapDecorator);
		}
		return finalRes;
	}
}