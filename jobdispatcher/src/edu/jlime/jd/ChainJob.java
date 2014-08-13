package edu.jlime.jd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import edu.jlime.client.JobContext;
import edu.jlime.jd.job.Job;

public class ChainJob<R> implements Job<Map<JobNode, R>> {

	private static final long serialVersionUID = -1099540720020480578L;

	private List<JobNode> rem;

	private Job<R> j;

	public ChainJob(Job<R> j, List<JobNode> remaining) {
		this.j = j;
		this.rem = remaining;
	}

	@Override
	public Map<JobNode, R> call(final JobContext env, JobNode peer)
			throws Exception {

		// System.out.println("Chaining from server "
		// + env.getCluster().getLocalPeer() + " to " + rem);

		final ConcurrentHashMap<JobNode, R> ret = new ConcurrentHashMap<>();
		JobNode local = env.getCluster().getLocalNode();

		ret.put(local, local.exec(j));

		if (!rem.isEmpty()) {
			final Semaphore sem = new Semaphore(-1);
			new Thread() {
				public void run() {
					ArrayList<JobNode> middle = new ArrayList<>(rem.subList(0,
							(int) Math.ceil(rem.size() / (double) 2)));
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
						ArrayList<JobNode> middle = new ArrayList<>(
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
