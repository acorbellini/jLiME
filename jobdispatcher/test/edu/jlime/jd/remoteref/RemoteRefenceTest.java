package edu.jlime.jd.remoteref;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.RemoteReference;
import edu.jlime.jd.job.Job;

public class RemoteRefenceTest {

	public static class RemoteReferenceJob implements
			Job<RemoteReference<String>> {

		@Override
		public RemoteReference<String> call(JobContext env, JobNode peer)
				throws Exception {
			RemoteReference<String> ref = new RemoteReference<String>("Hola!",
					env);
			return ref;
		}

	}

	public static void main(String[] args) throws Exception {
		JobCluster c = Client.build(1).getCluster();
		RemoteReference<String> ref = c.getAnyExecutor().exec(
				new RemoteReferenceJob());
		System.out.println(ref.get());
	}
}
