package edu.jlime.graphly.util;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;

import edu.jlime.graphly.client.TopMerger.TopComparator;
import edu.jlime.graphly.client.VertexIterator;
import edu.jlime.graphly.storenode.GraphlyStoreNode;
import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeI;
import edu.jlime.util.Pair;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.set.hash.TLongHashSet;

public class TopGatherer implements Gather<Set<Pair<Long, Float>>> {

	private String prop;
	private int max;
	private TLongHashSet v;

	public TopGatherer(String string, int i) {
		this.prop = string;
		this.max = i;
	}

	public TopGatherer(String string, int i, TLongHashSet vertices) {
		this(string, i);
		this.v = vertices;
	}

	@Override
	public Set<Pair<Long, Float>> gather(String graph, GraphlyStoreNode node)
			throws Exception {

		TreeSet<Pair<Long, Float>> sorted = new TreeSet<Pair<Long, Float>>(
				new TopComparator());

		if (v == null) {
			TLongFloatIterator it = node.getFloatIterator(graph, prop);
			while (it.hasNext()) {
				it.advance();
				long vid = it.key();
				float val = it.value();
				if (sorted.size() < max) {
					sorted.add(Pair.build(vid, val));
				} else {
					Pair<Long, Float> last = sorted.last();
					if (last.right < val) {
						sorted.remove(last);
						sorted.add(Pair.build(vid, val));
					}
				}
			}
			return sorted;
		}

		Iterator<Long> it = new TLongSetDecorator(v).iterator();

		while (it.hasNext()) {
			long vid = it.next();
			float val = node.getFloat(graph, vid, prop);
			if (sorted.size() < max) {
				sorted.add(Pair.build(vid, val));
			} else {
				Pair<Long, Float> last = sorted.last();
				if (last.right < val) {
					sorted.remove(last);
					sorted.add(Pair.build(vid, val));
				}
			}
		}
		return sorted;
	}
}
