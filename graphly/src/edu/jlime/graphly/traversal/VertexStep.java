package edu.jlime.graphly.traversal;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.jobs.VertexForkJoin;
import edu.jlime.jd.JobDispatcher;

public class VertexStep implements Step<long[], long[]> {

	private Dir dir;
	private GraphlyTraversal gt;

	public VertexStep(Dir dir, GraphlyTraversal gt) {
		this.dir = dir;
		this.gt = gt;
	}

	@Override
	public long[] exec(long[] before) throws Exception {
		JobDispatcher te = gt.getGraph().getJobClient();
		VertexForkJoin vfj = new VertexForkJoin(before, dir,
				(Mapper) gt.get("mapper"));
		long[] ret = vfj.exec(te.getCluster()).toArray();
		return ret;
	}
}
