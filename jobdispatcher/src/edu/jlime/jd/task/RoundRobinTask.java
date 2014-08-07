package edu.jlime.jd.task;

import java.util.HashMap;
import java.util.List;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;

public class RoundRobinTask<R> extends BroadcastTask<R> {

	public RoundRobinTask(List<? extends Job<R>> jobs, JobCluster c) {
		super(jobs, c);
	}

	@Override
	public <J extends Job<R>> HashMap<Job<R>, JobNode> split(
			List<JobNode> peers, List<J> jobs) {
		HashMap<Job<R>, JobNode> map = new HashMap<>();
		int count = 0;
		for (Job<R> job : jobs) {
			JobNode p = peers.get(count++ % peers.size());
			map.put(job, p);
		}
		return map;
	}

}
