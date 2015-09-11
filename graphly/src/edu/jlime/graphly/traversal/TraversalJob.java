package edu.jlime.graphly.traversal;

import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class TraversalJob implements Job<TraversalResult> {

	private Traversal g;

	public TraversalJob(Traversal graphlyTraversal) {
		this.g = graphlyTraversal;
	}

	@Override
	public TraversalResult call(JobContext env, Node peer) throws Exception {
		return g.exec();
	}

}
