package edu.jlime.graphly.traversal.count;

import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.set.hash.TLongHashSet;

public class ParallelStepJob implements Job<TraversalResult> {

	private TLongHashSet list;
	private Step t;
	private Traversal tr;

	public ParallelStepJob(TLongHashSet tLongHashSet, Step task, Traversal tr) {
		list = tLongHashSet;
		this.t = task;
		this.tr = tr;
	}

	@Override
	public TraversalResult call(JobContext env, Node peer) throws Exception {
		return tr.getGraph().v(list).set("mapper", tr.get("mapper")).addStep(t).exec();
	}

}
