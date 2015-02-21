package edu.jlime.graphly.traversal;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class VertexJob implements Job<long[]> {

	private static final long serialVersionUID = -3316802861448545540L;

	private long[] data;

	private Dir dir;

	private int max_edges;

	public VertexJob(Dir dir, int max_edges, long[] data) {
		this.dir = dir;
		this.data = data;
		this.max_edges = max_edges;
	}

	@Override
	public long[] call(JobContext ctx, ClientNode peer) throws Exception {
		Graphly g = (Graphly) ctx.getGlobal("graphly");
		return g.getEdges(dir, max_edges, data);
	}
}