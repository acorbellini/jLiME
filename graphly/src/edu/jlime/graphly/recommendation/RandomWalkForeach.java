package edu.jlime.graphly.recommendation;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.each.ForEach;
import gnu.trove.list.array.TLongArrayList;

final class RandomWalkForeach implements ForEach<long[]> {
	private int max_depth;
	private Dir[] dirs;
	private long[] subset;

	public RandomWalkForeach(int md, long[] subset, Dir[] dirs) {
		this.max_depth = md;
		if (dirs.length == 0)
			this.dirs = new Dir[] { Dir.OUT };
		else
			this.dirs = dirs;
		this.subset = subset;
	}

	@Override
	public long[] exec(long vid, Graphly g) throws Exception {
		TLongArrayList ret = new TLongArrayList();
		boolean done = false;
		long cursor = vid;
		while (ret.size() < max_depth && !done) {
			GraphlyTraversal tr = g.v(cursor);
			for (Dir dir : dirs) {
				tr.random(dir, subset);
			}
			long[] curr = tr.exec().vertices().toArray();
			if (curr.length == 0) {
				done = true;
			} else {
				cursor = curr[0];
				if (cursor == vid)
					done = true;
				else
					ret.add(cursor);
			}
		}
		return ret.toArray();
	}
}