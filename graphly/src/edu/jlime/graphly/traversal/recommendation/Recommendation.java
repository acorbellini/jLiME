package edu.jlime.graphly.traversal.recommendation;

import java.util.Arrays;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.CustomTraversal;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import gnu.trove.list.array.TLongArrayList;

public class Recommendation extends CustomTraversal {

	public Recommendation(GraphlyTraversal tr) {
		super(tr);
	}

	public Recommendation exploratoryCount(Dir... dirs) throws Exception {
		tr.save("target")
				.to(dirs[0])
				.save("first")
				.traverse(new String[] { "first", "target" },
						Arrays.copyOfRange(dirs, 1, dirs.length - 1))
				.count(dirs[dirs.length - 1]).filterBy("first", "target");
		return this;
	}

	public Recommendation randomwalk(int steps, int max_depth) {
		tr.repeat(steps, new ForEach<long[]>() {
			@Override
			public long[] exec(long vid, Graphly g) throws Exception {
				TLongArrayList ret = new TLongArrayList();
				int i = 0;
				GraphlyTraversal tr = g.v(vid);
				boolean done = false;
				while (i < max_depth && !done) {
					Long curr = (Long) tr.randomOut().exec();
					if (curr == null) {
						done = true;
					}
				}
				return ret.toArray();
			}
		});
		return this;
	}
}
