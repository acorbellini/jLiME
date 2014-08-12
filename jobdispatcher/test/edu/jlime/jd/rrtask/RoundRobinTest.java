package edu.jlime.jd.rrtask;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.jd.task.RoundRobinTask;

public class RoundRobinTest {

	private static class HelloJob implements Job<String> {

		private static final long serialVersionUID = -8211563062939980181L;

		private String msg;

		public HelloJob(String string) {
			this.msg = string;
		}

		@Override
		public String call(JobContext env, JobNode peer) throws Exception {
			// Thread.sleep((long) (Math.random() * 3000));
			return msg + env.getCluster().getLocalPeer().getID();
		}

	}

	@Test
	public void roundRobin() throws Exception {
		main(new String[] {});
	}

	public static void main(String[] args) throws Exception {
		// DEFServer.jLiME();
		List<Job<String>> jobs = new ArrayList<>();

		jobs.add(new HelloJob("Hey "));
		jobs.add(new HelloJob("Ho "));
		jobs.add(new HelloJob("Lets "));
		jobs.add(new HelloJob("Go "));
		Client cli = Client.build(3);
		RoundRobinTask<String> task = new RoundRobinTask<>(jobs,
				cli.getCluster());

		// task.setMaxPeers(2);

		task.execute(new ResultListener<String, Void>() {
			@Override
			public void onSuccess(String result) {
				System.out.println(result);
			}

			@Override
			public Void onFinished() {
				System.out.println("Finished");
				return null;
			}

			@Override
			public void onFailure(Exception res) {
				System.out.println("Damn it");
			}
		});
		cli.close();
	}
}
