package edu.jlime.jd.remoteref;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.RemoteReference;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class RemoteRefenceTest {

	public static class RemoteReferenceJob implements
			Job<RemoteReference<String>> {

		@Override
		public RemoteReference<String> call(JobContext env, ClientNode peer)
				throws Exception {
			RemoteReference<String> ref = new RemoteReference<String>("Hola!",
					env);
			return ref;
		}

	}

	public static void main(String[] args) throws Exception {
		ClientCluster c = Client.build(1).getCluster();
		RemoteReference<String> ref = c.getAnyExecutor().exec(
				new RemoteReferenceJob());
		System.out.println(ref.get());
	}
}
