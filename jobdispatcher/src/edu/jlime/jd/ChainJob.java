package edu.jlime.jd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class ChainJob<R> implements Job<Map<ClientNode, R>> {

	private static final long serialVersionUID = -1099540720020480578L;

	private List<ClientNode> rem;

	private Job<R> j;

	public ChainJob(Job<R> j, List<ClientNode> remaining) {
		this.j = j;
		this.rem = remaining;
	}

	@Override
	public Map<ClientNode, R> call(final JobContext env, ClientNode peer)
			throws Exception {

		// System.out.println("Chaining from server "
		// + env.getCluster().getLocalPeer() + " to " + rem);

		final ConcurrentHashMap<ClientNode, R> ret = new ConcurrentHashMap<>();
		ClientNode local = env.getCluster().getLocalNode();

		ret.put(local, local.exec(j));

		if (!rem.isEmpty()) {
			final Semaphore sem = new Semaphore(-1);
			new Thread() {
				public void run() {
					ArrayList<ClientNode> middle = new ArrayList<>(rem.subList(
							0, (int) Math.ceil(rem.size() / (double) 2)));
					try {
						ret.putAll(env.getCluster().chain(middle, j));
					} catch (Exception e) {
						e.printStackTrace();
					}
					sem.release();
				};
			}.start();
			if (rem.size() == 1)
				sem.release();
			else
				new Thread() {
					public void run() {
						ArrayList<ClientNode> middle = new ArrayList<>(
								rem.subList((int) Math.ceil(rem.size()
										/ (double) 2), rem.size()));
						try {
							ret.putAll(env.getCluster().chain(middle, j));
						} catch (Exception e) {
							e.printStackTrace();
						}
						sem.release();
					};
				}.start();
			sem.acquire();
		}
		return ret;
	}
}
