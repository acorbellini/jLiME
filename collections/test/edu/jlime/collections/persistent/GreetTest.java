package edu.jlime.collections.persistent;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;

public class GreetTest implements Serializable {

	private static final long serialVersionUID = 6972258889652708237L;

	public static class Greet implements Job<Void> {

		private static final long serialVersionUID = -4564428520013674262L;

		@Override
		public Void call(JobContext ctx, ClientNode peer) throws Exception {
			Logger log = Logger.getLogger(Greet.class);
			log.info("Hola!");
			return null;
		}
	}

	@Test
	public void testGreet() throws Exception {
		ClientCluster cluster = Client.build().getCluster();
		cluster.getAnyExecutor().exec(new Greet());
	}
}
