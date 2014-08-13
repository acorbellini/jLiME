package edu.jlime.jd.txrx;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.job.RunJob;

public class TransmittedReceivedTest {

	public static class MessageJob extends RunJob {

		private static final long serialVersionUID = 3372537223697499205L;

		private String msg;

		private int[] data;

		public MessageJob(String msg, int[] data) {
			this.msg = msg;
			this.data = data;
		}

		@Override
		public void run(JobContext env, JobNode origin) throws Exception {
			System.out.println(msg);
		}

	}

	public static class TransmissionJob extends RunJob {

		private static final long serialVersionUID = 4843051203459150275L;

		@Override
		public void run(JobContext env, JobNode origin) throws Exception {
			// ArrayList<Peer> exec = env.getCluster().getExecutors();
			//
			// for (int i = 1; i <= 1000; i++) {
			// final Semaphore sem = new Semaphore(-exec.size() + 1);
			//
			// for (Peer peer : exec) {
			// peer.execAsync(new MessageJob("Message From "
			// + env.getCluster().getLocal().getIPv4() + " round "
			// + i), new ResultManager<Boolean>() {
			//
			// @Override
			// protected void handleException(Exception res,
			// String jobID, Peer fromID) {
			// res.printStackTrace();
			// sem.release();
			// }
			//
			// @Override
			// protected void handleResult(Boolean res, String jobID,
			// Peer fromID) {
			// sem.release();
			// }
			// });
			// }
			// System.out.println("Waiting for semaphore, round " + i);
			// sem.acquire();
			// }
			System.out.println("Executing Transmission Job.");
			JobCluster cluster = env.getCluster();

			for (int i = 1; i <= 50; i++) {
				int[] data = new int[8000];
				for (int j = 0; j < data.length; j++) {
					data[i] = (int) (Math.random() * 1000000);
				}
				cluster.broadcast(new MessageJob("Message From "
						+ env.getCluster().getLocalNode() + " round " + i, data));
			}
		}
	}

	@Test
	public void txrx() throws Exception {
		long init = System.currentTimeMillis();
		Client cli = Client.build(2);
		ArrayList<JobNode> exec = cli.getCluster().getExecutors();
		final Semaphore sem = new Semaphore(-exec.size() + 1);
		for (JobNode peer : exec) {
			System.out.println("Executing transmission Job on " + peer);
			peer.execAsync(new TransmissionJob(), new ResultManager<Boolean>() {

				@Override
				public void handleException(Exception res, String jobID,
						JobNode fromID) {
					sem.release();
				}

				@Override
				public void handleResult(Boolean res, String jobID,
						JobNode fromID) {
					sem.release();
				}
			});
		}
		sem.acquire();

		long end = System.currentTimeMillis();

		System.out.println(end - init);
		cli.close();
	}

	public static void main(String[] args) throws Exception {
		new TransmittedReceivedTest().txrx();
	}
}
