package edu.jlime.graphly.traversal;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class TraversalJob implements Job {

	private GraphlyTraversal g;

	public TraversalJob(GraphlyTraversal graphlyTraversal) {
		this.g = graphlyTraversal;
	}

	@Override
	public Object call(JobContext env, ClientNode peer) throws Exception {
		return g.exec();
	}

}