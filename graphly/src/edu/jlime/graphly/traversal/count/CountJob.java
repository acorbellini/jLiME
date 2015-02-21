package edu.jlime.graphly.traversal.count;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.map.hash.TLongIntHashMap;

public class CountJob implements Job<TLongIntHashMap> {

	private Dir dir;
	private long[] data;

	public CountJob(Dir dir, long[] array) {
		this.dir = dir;
		this.data = array;
	}

	@Override
	public TLongIntHashMap call(JobContext ctx, ClientNode peer)
			throws Exception {
		Graphly g = (Graphly) ctx.getGlobal("graphly");
		TLongIntHashMap l = g.countEdges(dir, data);
		return l;
	}
}
