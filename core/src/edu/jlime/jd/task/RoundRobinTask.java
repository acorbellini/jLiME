package edu.jlime.jd.task;

import java.util.HashMap;
import java.util.List;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Node;
import edu.jlime.jd.job.Job;

public class RoundRobinTask<T> extends BroadcastTask<T> {

	public RoundRobinTask(List<? extends Job<T>> jobs, ClientCluster c) {
		super(jobs, c);
	}

	@Override
	public <J extends Job<T>> HashMap<Job<T>, Node> split(List<Node> peers, List<J> jobs) {
		HashMap<Job<T>, Node> map = new HashMap<>();
		int count = 0;
		for (Job<T> job : jobs) {
			Node p = peers.get(count++ % peers.size());
			map.put(job, p);
		}
		return map;
	}

}
