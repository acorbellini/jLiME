package edu.jlime.jd.task;

import java.util.HashMap;
import java.util.List;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;

public class RoundRobinTask<T> extends BroadcastTask<T> {

	public RoundRobinTask(List<? extends Job<T>> jobs, JobCluster c) {
		super(jobs, c);
	}

	@Override
	public <J extends Job<T>> HashMap<Job<T>, JobNode> split(
			List<JobNode> peers, List<J> jobs) {
		HashMap<Job<T>, JobNode> map = new HashMap<>();
		int count = 0;
		for (Job<T> job : jobs) {
			JobNode p = peers.get(count++ % peers.size());
			map.put(job, p);
		}
		return map;
	}

}
