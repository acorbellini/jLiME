package edu.jlime.jd.jobsumit;

import org.junit.Test;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class JobSubmissionTest {

	public static class SplitJob implements Job<String[]> {

		private static final long serialVersionUID = -6263126816241078830L;

		private String toSplit;

		public SplitJob(String toSplit) {
			this.toSplit = toSplit;
		}

		@Override
		public String[] call(JobContext env, ClientNode peer) throws Exception {
			return toSplit.split("\\s");
		}
	}

	@Test
	public void submitTest() throws Exception {
		ClientCluster c = Client.build().getCluster();
		String[] res = c.getAnyExecutor().exec(new SplitJob("Hola que tal"));
		for (String string : res) {
			System.out.println(string);
		}

	}
}
