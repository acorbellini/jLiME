package edu.jlime.graphly.util;

import java.util.Iterator;

import edu.jlime.graphly.storenode.StoreNodeImpl;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.set.hash.TLongHashSet;

public class SumFloatPropertiesGather implements Gather<Float> {
	private String prop;
	private TLongHashSet v;

	public SumFloatPropertiesGather(String prop) {
		this.prop = prop;
	}

	public SumFloatPropertiesGather(String string, TLongHashSet vertices) {
		this(string);
		this.v = vertices;
	}

	@Override
	public Float gather(String graph, StoreNodeImpl node) throws Exception {

		if (v == null) {
			float ret = 0f;
			TLongFloatIterator it = node.getFloatIterator(graph, prop);
			while (it.hasNext()) {
				it.advance();
				float val = it.value();
				ret += val;
			}
			return ret;
		}
		float ret = 0f;
		Iterator<Long> it = new TLongSetDecorator(v).iterator();
		while (it.hasNext()) {
			long vid = it.next();
			ret += node.getFloat(graph, vid, prop);
		}
		return ret;
	}
}
