package edu.jlime.graphly.util;

import java.util.Set;
import java.util.TreeSet;

import edu.jlime.graphly.client.TopMerger.TopComparator;
import edu.jlime.graphly.storenode.GraphlyStoreNode;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class TopGatherer implements Gather<Set<Pair<Long, Float>>> {

	private String prop;
	private int max;

	public TopGatherer(String string, int i) {
		this.prop = string;
		this.max = i;
	}

	@Override
	public Set<Pair<Long, Float>> gather(String graph, GraphlyStoreNode node)
			throws Exception {

		TreeSet<Pair<Long, Float>> sorted = new TreeSet<Pair<Long, Float>>(
				new TopComparator());

		TLongArrayList v = node.getVertices(graph, Long.MIN_VALUE,
				Integer.MAX_VALUE, true);

		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			float val = node.getFloat(graph, vid, prop);
			if (sorted.size() < max) {
				sorted.add(Pair.build(vid, val));
			} else {
				Pair<Long, Float> first = sorted.first();
				if (first.right < val) {
					sorted.remove(first);
					sorted.add(Pair.build(vid, val));
				}
			}
		}
		return sorted;
	}
}
