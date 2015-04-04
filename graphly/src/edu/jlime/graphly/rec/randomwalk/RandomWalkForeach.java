package edu.jlime.graphly.rec.randomwalk;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.each.ForEach;
import gnu.trove.list.array.TLongArrayList;

public class RandomWalkForeach implements ForEach<long[]> {
	private float max_depth;
	private Dir[] dirs;
	private long[] subset;

	public RandomWalkForeach(float max, long[] subset, Dir... out) {
		this.max_depth = max;
		if (out.length == 0)
			this.dirs = new Dir[] { Dir.OUT };
		else
			this.dirs = out;
		this.subset = subset;
	}

	@Override
	public long[] exec(long vid, GraphlyGraph g) throws Exception {
		if (subset != null && subset.length > 0) {
			SubGraph sg = g.getSubGraph("random-walk", subset);
			TLongArrayList ret = new TLongArrayList();
			boolean done = false;
			long cursor = vid;
			while (((max_depth < 1f && Math.random() > max_depth) || ret.size() < max_depth)
					&& !done) {
				for (Dir dir : dirs) {
					cursor = sg.getRandomEdge(dir, cursor);
					if (cursor < 0)
						break;
				}
				if (cursor < 0) {
					done = true;
				} else if (cursor == vid)
					done = true;
				else
					ret.add(cursor);
			}
			if (ret.isEmpty())
				return null;
			return ret.toArray();
		}
		TLongArrayList ret = new TLongArrayList();
		boolean done = false;
		long cursor = vid;
		while (((max_depth < 1f && Math.random() > max_depth) || ret.size() < max_depth)
				&& !done) {
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
		if (ret.isEmpty())
			return null;
		return ret.toArray();
	}
}