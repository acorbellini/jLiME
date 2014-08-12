package edu.jlime.jd.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.SetEnvironment;
import edu.jlime.jd.job.Job;

public abstract class BroadcastTask<T> extends TaskBase<T> {

	private JobCluster cluster;

	private int maxPeers = -1;

	ArrayList<JobNode> peers;

	private List<? extends Job<T>> jobs;

	public BroadcastTask(List<? extends Job<T>> jobs, JobCluster c) {
		this.cluster = c;
		peers = c.getExecutors();
		this.jobs = jobs;
	}

	public void setMaxPeers(int maxPeers) {
		this.maxPeers = maxPeers;
	}

	public void set(String k, Object v, boolean chain) {
		HashSet<JobNode> peers = new HashSet<>(getMap().values());
		SetEnvironment senv = new SetEnvironment(k, v);
		try {
			if (chain)
				cluster.chain(peers, senv);
			else
				cluster.mcastAsync(peers, senv);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void set(String k, Object v) {
		set(k, v, false);
	}

	@Override
	protected Map<Job<T>, JobNode> getMap() {
		return split(limitPeers(peers), jobs);
	}

	private List<JobNode> limitPeers(ArrayList<JobNode> peers) {
		if (maxPeers == -1)
			return peers;
		ArrayList<JobNode> copy = new ArrayList<>(peers);
		ArrayList<JobNode> limited = new ArrayList<>();
		for (int i = 0; i < maxPeers; i++)
			limited.add(copy.remove((int) (Math.random() * copy.size())));
		return limited;
	}

	public abstract <J extends Job<T>> HashMap<Job<T>, JobNode> split(
			List<JobNode> peers, List<J> jobs);

	public static String getID(String k, String sharedID) {
		return k + "-" + sharedID;
	}
}
