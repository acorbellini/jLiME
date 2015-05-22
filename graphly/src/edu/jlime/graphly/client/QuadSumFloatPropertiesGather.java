package edu.jlime.graphly.client;

import edu.jlime.graphly.storenode.GraphlyStoreNode;
import edu.jlime.graphly.util.Gather;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class QuadSumFloatPropertiesGather implements Gather<Float> {

	private String prop;

	public QuadSumFloatPropertiesGather(String string) {
		this.prop = string;
	}

	@Override
	public Float gather(String graph, GraphlyStoreNode node) throws Exception {
		float ret = 0f;
		TLongArrayList v = node.getVertices(graph, Long.MIN_VALUE,
				Integer.MAX_VALUE, true);
		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			float float1 = node.getFloat(graph, vid, prop);
			ret += float1 * float1;
		}
		return ret;
	}

}
