package edu.jlime.graphly.client;

import java.util.Iterator;

import com.google.common.collect.Lists;

import edu.jlime.graphly.storenode.GraphlyStoreNode;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeI;
import edu.jlime.graphly.util.Gather;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.set.hash.TLongHashSet;

public class QuadSumFloatPropertiesGather implements Gather<Float> {

	private String prop;
	private TLongHashSet v;

	public QuadSumFloatPropertiesGather(String string) {
		this.prop = string;
	}

	public QuadSumFloatPropertiesGather(String string, TLongHashSet vertices) {
		this(string);
		this.v = vertices;
	}

	@Override
	public Float gather(String graph, GraphlyStoreNode node) throws Exception {
		float ret = 0f;
		Iterator<Long> it;
		if (v == null)
			it = new VertexIterator(graph,
					Lists.newArrayList((GraphlyStoreNodeI) node), 100000);
		else
			it = new TLongSetDecorator(v).iterator();
		while (it.hasNext()) {
			long vid = it.next();
			float float1 = node.getFloat(graph, vid, prop);
			ret += float1 * float1;
		}
		return ret;
	}

}
