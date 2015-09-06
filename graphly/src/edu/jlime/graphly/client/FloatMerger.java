package edu.jlime.graphly.client;

import java.util.List;

import gnu.trove.map.hash.TLongFloatHashMap;

public class FloatMerger implements GatherMerger<TLongFloatHashMap> {

	@Override
	public TLongFloatHashMap merge(List<TLongFloatHashMap> merge) {
		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (TLongFloatHashMap r : merge) {
			ret.putAll(r);
		}
		return ret;
	}

}
