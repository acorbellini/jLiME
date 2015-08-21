package edu.jlime.graphly.client;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import edu.jlime.util.Pair;

public class TopMerger implements GatherMerger<Set<Pair<Long, Float>>> {

	public static final class TopComparator implements
			Comparator<Pair<Long, Float>>, Serializable {
		@Override
		public int compare(Pair<Long, Float> o1, Pair<Long, Float> o2) {
			int comp = o1.right.compareTo(o2.right) * -1;
			if (comp == 0)
				return o1.left.compareTo(o2.left);
			return comp;
		}
	}

	private int top;

	public TopMerger(int top) {
		this.top = top;
	}

	@Override
	public Set<Pair<Long, Float>> merge(List<Set<Pair<Long, Float>>> merge) {
		TreeSet<Pair<Long, Float>> sorted = new TreeSet<Pair<Long, Float>>(
				new TopComparator());

		Iterator<Set<Pair<Long, Float>>> it = merge.iterator();
		while (it.hasNext()) {
			Set<Pair<Long, Float>> vid = it.next();
			for (Pair<Long, Float> pair : vid) {
				if (sorted.size() < top) {
					sorted.add(pair);
				} else {
					Pair<Long, Float> last = sorted.last();
					if (last.right < pair.right) {
						sorted.remove(last);
						sorted.add(pair);
					}
				}
			}
		}
		return sorted;
	}

}
