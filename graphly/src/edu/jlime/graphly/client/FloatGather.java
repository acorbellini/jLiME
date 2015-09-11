package edu.jlime.graphly.client;

import edu.jlime.graphly.storenode.StoreNodeImpl;
import edu.jlime.graphly.util.Gather;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class FloatGather implements Gather<TLongFloatHashMap> {

	private String prop;
	private TLongHashSet vertices;

	public FloatGather(String k, TLongHashSet vertices) {
		this.vertices = vertices;
		this.prop = k;
	}

	@Override
	public TLongFloatHashMap gather(String graph, StoreNodeImpl node) throws Exception {

		TLongFloatHashMap ret = new TLongFloatHashMap();
		if (vertices == null) {
			TLongFloatIterator it = node.getFloatIterator(graph, prop);
			while (it.hasNext()) {
				it.advance();
				long vid = it.key();
				float val = it.value();
				ret.put(vid, val);
			}
			return ret;
		}
		TLongIterator it = vertices.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			ret.put(vid, node.getFloat(graph, vid, prop));
		}
		return ret;

	}

}
