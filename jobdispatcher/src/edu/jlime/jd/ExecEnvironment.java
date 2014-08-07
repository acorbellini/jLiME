package edu.jlime.jd;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.client.JobContextImpl;
import edu.jlime.core.cluster.Peer;

public class ExecEnvironment {

	ConcurrentHashMap<String, JobContextImpl> clientEnvs = new ConcurrentHashMap<>();

	Logger log = Logger.getLogger(ExecEnvironment.class);

	JobDispatcher srv;

	public ExecEnvironment(JobDispatcher jobDispatcher) {
		this.srv = jobDispatcher;
	}

	public synchronized JobContextImpl getClientEnv(String client) {
		JobContextImpl env = clientEnvs.get(client);
		if (env == null) {
			if (log.isDebugEnabled())
				log.debug("Creating new client environment for client "
						+ client);
			env = new JobContextImpl(new JobCluster(srv, client), client);
			clientEnvs.put(client, env);
		}
		return env;
	}

	public void remove(Peer srv) {
		JobContext cliEnv = clientEnvs.remove(srv.getID());
		if (cliEnv != null)
			cliEnv.stop();
	}
}
