package edu.jlime.jd.txrx;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
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
		public void run(JobContext env, ClientNode origin) throws Exception {
			// System.out.println(msg);
		}

	}

	public static class TransmissionJob extends RunJob {

		private static final long serialVersionUID = 4843051203459150275L;

		@Override
		public void run(JobContext env, ClientNode origin) throws Exception {
			System.out.println("Executing Transmission Job.");
			ClientCluster cluster = env.getCluster();

			for (int i = 1; i <= 200; i++) {
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
		ArrayList<ClientNode> exec = cli.getCluster().getExecutors();
		final Semaphore sem = new Semaphore(-exec.size() + 1);
		for (ClientNode peer : exec) {
			System.out.println("Executing transmission Job on " + peer);
			peer.execAsync(new TransmissionJob(), new ResultManager<Boolean>() {

				@Override
				public void handleException(Exception res, String jobID,
						ClientNode fromID) {
					sem.release();
				}

				@Override
				public void handleResult(Boolean res, String jobID,
						ClientNode fromID) {
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
