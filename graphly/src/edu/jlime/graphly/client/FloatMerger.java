package edu.jlime.graphly.client;

import gnu.trove.map.hash.TLongFloatHashMap;

import java.util.List;

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
