package edu.jlime.graphly.traversal.count;

import edu.jlime.graphly.GraphlyCount;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class CountJob implements Job<GraphlyCount> {

	private Dir dir;
	private long[] data;
	int max_edges;

	public CountJob(Dir dir, int max_edges, long[] array) {
		this.dir = dir;
		this.data = array;
		this.max_edges = max_edges;
	}

	@Override
	public GraphlyCount call(JobContext ctx, ClientNode peer) throws Exception {
		Graphly g = (Graphly) ctx.getGlobal("graphly");
		GraphlyCount l = g.countEdges(dir, max_edges, data);
		return l;
	}
}
