package edu.jlime.graphly.util;

import edu.jlime.graphly.storenode.GraphlyStoreNode;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class SumFloatPropertiesGather implements Gather<Float> {
	private String prop;

	public SumFloatPropertiesGather(String prop) {
		this.prop = prop;
	}

	@Override
	public Float gather(String graph, GraphlyStoreNode node) throws Exception {
		float ret = 0f;
		TLongArrayList v = node.getVertices(graph, Long.MIN_VALUE,
				Integer.MAX_VALUE, true);
		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			ret += node.getFloat(graph, vid, prop);
		}
		return ret;
	}
}
