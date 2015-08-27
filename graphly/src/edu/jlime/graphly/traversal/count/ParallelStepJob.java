package edu.jlime.graphly.traversal.count;

import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.set.hash.TLongHashSet;

public class ParallelStepJob implements Job<TraversalResult> {

	private TLongHashSet list;
	private Step t;
	private GraphlyTraversal tr;

	public ParallelStepJob(TLongHashSet tLongHashSet, Step task,
			GraphlyTraversal tr) {
		list = tLongHashSet;
		this.t = task;
		this.tr = tr;
	}

	@Override
	public TraversalResult call(JobContext env, ClientNode peer)
			throws Exception {
		return tr.getGraph().v(list).set("mapper", tr.get("mapper")).addStep(t)
				.exec();
	}

}
