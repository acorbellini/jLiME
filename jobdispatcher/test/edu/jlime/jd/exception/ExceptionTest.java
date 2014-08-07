package edu.jlime.jd.exception;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.RunJob;

public class ExceptionTest {

	public static class ExceptionJob extends RunJob {

		private static final long serialVersionUID = 8685143678806165178L;

		@Override
		public void run(JobContext env, JobNode origin) throws Exception {
			throw new Exception("Remote Exception.");
		}

	}

	@Test
	public void exceptionTest() throws Exception {
		Client cli = Client.build();
		// System.out.println("Sync Exception");
		// cli.getCluster().getAnyExecutor().exec(new ExceptionJob());
		System.out.println("Async Exception");
		cli.getCluster().getAnyExecutor().execAsync(new ExceptionJob());
		System.out.println("Termino");
		while (true)
			Thread.sleep(5000000);
	}
}
