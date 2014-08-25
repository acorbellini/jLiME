package edu.jlime.jd.shared;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.jd.task.RoundRobinTask;

public class SharedTest {

	private static class SharedJob implements Job<String> {

		private static final long serialVersionUID = 187854003466950459L;

		private String msg;

		public SharedJob(String string) {
			this.msg = string;
		}

		@Override
		public String call(JobContext env, ClientNode peer) throws Exception {
			Thread.sleep((long) (Math.random() * 3000));
			return msg + " - " + env.waitFor("data");
		}

	}

	@Test
	public void sharedTest() throws Exception {
		// DEFServer.jLiME();
		// DEFServer.jLiME();

		List<SharedJob> jobs = new ArrayList<>();
		jobs.add(new SharedJob("1"));
		jobs.add(new SharedJob("2"));
		jobs.add(new SharedJob("3"));
		jobs.add(new SharedJob("4"));
		RoundRobinTask<String> task = new RoundRobinTask<String>(jobs, Client
				.build().getCluster());

		task.set("data", "First");
		List<SharedJob> jobs2 = new ArrayList<>();
		jobs2.add(new SharedJob("a"));
		jobs2.add(new SharedJob("b"));
		jobs2.add(new SharedJob("c"));
		jobs2.add(new SharedJob("d"));
		RoundRobinTask<String> task2 = new RoundRobinTask<String>(jobs2, Client
				.build().getCluster());
		task2.set("data", "Segundo");
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

			}
		});

		task2.execute(new ResultListener<String, Void>() {
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

			}
		});
	}
}
