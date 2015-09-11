package edu.jlime.jd.txrx;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.job.RunJob;
import edu.jlime.jd.server.JobServer;

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
		public void run(JobContext env, Node origin) throws Exception {
			// System.out.println(msg);
		}

	}

	public static class TransmissionJob extends RunJob {

		private static final long serialVersionUID = 4843051203459150275L;

		@Override
		public void run(JobContext env, Node origin) throws Exception {
			System.out.println("\n********************************************************\n");
			System.out.println("Executing Transmission Job for " + origin);
			System.out.println("\n********************************************************\n");
			ClientCluster cluster = env.getCluster();

			for (int i = 1; i <= 10; i++) {
				System.out.println("\n********************************************************\n");
				System.out.println("Executing round " + i + " for " + origin);
				System.out.println("\n********************************************************\n");
				int[] data = new int[8000];
				for (int j = 0; j < data.length; j++) {
					data[i] = (int) (Math.random() * 1000000);
				}
				cluster.broadcast(
						new MessageJob("Message From " + env.getCluster().getLocalNode() + " round " + i, data));
			}
		}
	}

	@Test
	public void txrx() throws Exception {
		JobServer.jLiME().start();
		JobServer.jLiME().start();

		// for (int i = 0; i < 50; i++) {
		// System.out
		// .println("\n*****************************************************************\n");
		// System.out.println("New Execution : " + i + "\n\n");
		// System.out
		// .println("\n*****************************************************************\n");

		long init = System.currentTimeMillis();
		Client cli = Client.build(2);
		ClientCluster cluster = cli.getCluster();
		System.out.println("Local Peer: " + cluster.getLocalNode());

		ArrayList<Node> exec = cluster.getExecutors();
		// final Semaphore sem = new Semaphore(-exec.size() + 1);
		final Semaphore sem = new Semaphore(0);
		Node peer = exec.get(0);
		// for (ClientNode peer : exec) {
		System.out.println("Executing transmission Job on " + peer);
		peer.execAsync(new TransmissionJob(), new ResultManager<Boolean>() {

			@Override
			public void handleException(Exception res, String jobID, Node fromID) {
				System.out.println("Received Exception ");
				res.printStackTrace();
				sem.release();
			}

			@Override
			public void handleResult(Boolean res, String jobID, Node fromID) {
				System.out.println("Received Result  from " + fromID);
				sem.release();
			}
		});
		// }
		sem.acquire();

		long end = System.currentTimeMillis();

		System.out.println(end - init);
		cli.close();
		// }
	}

	public static void main(String[] args) throws Exception {
		new TransmittedReceivedTest().txrx();
	}
}
