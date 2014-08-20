package edu.jlime.jd.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.SetEnvironment;
import edu.jlime.jd.job.Job;

public abstract class BroadcastTask<T> extends TaskBase<T> {

	private ClientCluster cluster;

	private int maxPeers = -1;

	ArrayList<ClientNode> peers;

	private List<? extends Job<T>> jobs;

	public BroadcastTask(List<? extends Job<T>> jobs, ClientCluster c) {
		this.cluster = c;
		peers = c.getExecutors();
		this.jobs = jobs;
	}

	public void setMaxPeers(int maxPeers) {
		this.maxPeers = maxPeers;
	}

	public void set(String k, Object v, boolean chain) {
		HashSet<ClientNode> peers = new HashSet<>(getMap().values());
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
	protected Map<Job<T>, ClientNode> getMap() {
		return split(limitPeers(peers), jobs);
	}

	private List<ClientNode> limitPeers(ArrayList<ClientNode> peers) {
		if (maxPeers == -1)
			return peers;
		ArrayList<ClientNode> copy = new ArrayList<>(peers);
		ArrayList<ClientNode> limited = new ArrayList<>();
		for (int i = 0; i < maxPeers; i++)
			limited.add(copy.remove((int) (Math.random() * copy.size())));
		return limited;
	}

	public abstract <J extends Job<T>> HashMap<Job<T>, ClientNode> split(
			List<ClientNode> peers, List<J> jobs);

	public static String getID(String k, String sharedID) {
		return k + "-" + sharedID;
	}
}
