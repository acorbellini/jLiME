package edu.jlime.graphly.util;

import edu.jlime.graphly.client.VertexIterator;
import edu.jlime.graphly.storenode.GraphlyStoreNode;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeI;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Iterator;

import com.google.common.collect.Lists;

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
			ret += node.getFloat(graph, vid, prop);
		}
		return ret;
	}
}
