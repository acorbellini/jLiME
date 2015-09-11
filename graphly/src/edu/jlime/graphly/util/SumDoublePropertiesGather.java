package edu.jlime.graphly.util;

import edu.jlime.graphly.storenode.StoreNodeImpl;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class SumDoublePropertiesGather implements Gather<Double> {

	private String prop;

	public SumDoublePropertiesGather(String string) {
		this.prop = string;
	}

	@Override
	public Double gather(String graph, StoreNodeImpl node) throws Exception {
		double ret = 0f;
		TLongArrayList v = node.getVertices(graph, Long.MIN_VALUE, Integer.MAX_VALUE, true);
		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			ret += node.getDouble(graph, vid, prop);
		}
		return ret;
	}

}
