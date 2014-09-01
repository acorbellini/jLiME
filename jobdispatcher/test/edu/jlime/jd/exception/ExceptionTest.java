package edu.jlime.jd.exception;

import org.junit.Test;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;
import edu.jlime.jd.server.JobServer;

public class ExceptionTest {

	public static class ExceptionJob extends RunJob {

		private static final long serialVersionUID = 8685143678806165178L;

		@Override
		public void run(JobContext env, ClientNode origin) throws Exception {
			throw new Exception("Remote Exception.");
		}

	}

	@Test
	public void exceptionTest() throws Exception {
		JobServer server = JobServer.jLiME();
		server.start();
		JobServer server2 = JobServer.jLiME();
		server2.start();
		JobServer server3 = JobServer.jLiME();
		server3.start();
		Client cli = Client.build(3);
		// try {
		// System.out.println("Sync Exception");
		// cli.getCluster().getAnyExecutor().exec(new ExceptionJob());
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// try {
		// System.out.println("Async Exception");
		// cli.getCluster().getAnyExecutor().execAsync(new ExceptionJob());
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// System.out.println("Termino");
		//
		// Thread.sleep(4000);

		try {
			System.out.println("Broadcast Sync Exception");
			cli.getCluster().broadcast(new ExceptionJob());
		} catch (Exception e) {
			e.printStackTrace();
		}

//		try {
//			System.out.println("Broadcast Async Exception");
//			cli.getCluster().broadcastAsync(new ExceptionJob());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		Thread.sleep(4000);
		
		cli.close();
		server.stop();
		server2.stop();
		server3.stop();
	}
}
