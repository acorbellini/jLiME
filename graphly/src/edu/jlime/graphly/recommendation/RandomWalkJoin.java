package edu.jlime.graphly.recommendation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.graphly.traversal.Join;
import gnu.trove.decorator.TLongFloatMapDecorator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

final class RandomWalkJoin implements Join<List<long[]>, Map<Long, Float>> {

	private TLongFloatMapDecorator normalize(List<long[]> input) {
		TLongFloatHashMap res = new TLongFloatHashMap();
		int cont = 0;
		for (long[] ls : input) {
			for (long l : ls) {
				res.adjustOrPutValue(l, 1, 1);
				cont++;
			}
		}

		long[] keys = res.keys();
		for (long l : keys) {
			res.put(l, res.get(l) / cont);
		}

		TLongFloatMapDecorator tLongFloatMapDecorator = new TLongFloatMapDecorator(
				res);
		return tLongFloatMapDecorator;
	}

	@Override
	public TLongObjectHashMap<Map<Long, Float>> join(
			TLongObjectHashMap<List<long[]>> before) {
		TLongObjectHashMap<Map<Long, Float>> finalRes = new TLongObjectHashMap<>();
		TLongObjectIterator<List<long[]>> it = before.iterator();
		while (it.hasNext()) {
			it.advance();
			long vid = it.key();
			List<long[]> input = it.value();
			TLongFloatMapDecorator tLongFloatMapDecorator = normalize(input);
			finalRes.put(vid, tLongFloatMapDecorator);
		}
		return finalRes;
	}
}