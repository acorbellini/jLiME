package edu.jlime.graphly.util;

import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.TopMerger.TopComparator;
import edu.jlime.graphly.storenode.GraphlyStoreNode;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
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
		Logger log = Logger.getLogger(TopGatherer.class);
		long init = System.currentTimeMillis();
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
		} else {

			TLongIterator it = v.iterator();
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
		}

		log.info("Finished top gatherer in "
				+ (System.currentTimeMillis() - init) + "ms");
		return sorted;
	}
}
