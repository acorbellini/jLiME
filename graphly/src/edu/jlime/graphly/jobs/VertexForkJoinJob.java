package edu.jlime.graphly.jobs;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class VertexForkJoinJob implements Job<long[]> {

	private static final long serialVersionUID = -3316802861448545540L;

	private long[] data;

	private Dir dir;

	public VertexForkJoinJob(Dir dir, long[] data) {
		this.dir = dir;
		this.data = data;
	}

	@Override
	public long[] call(JobContext ctx, ClientNode peer) throws Exception {
		Graphly g = (Graphly) ctx.getGlobal("graphly");
		return g.getEdges(dir, data);
	}
}