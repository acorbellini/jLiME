package edu.jlime.jd;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.client.JobContextImpl;

public class ExecEnvironment {

	ConcurrentHashMap<Peer, JobContextImpl> clientEnvs = new ConcurrentHashMap<>();

	Logger log = Logger.getLogger(ExecEnvironment.class);

	Dispatcher srv;

	public ExecEnvironment(Dispatcher jobDispatcher) {
		this.srv = jobDispatcher;
	}

	public synchronized JobContext getClientEnv(Peer client) {
		JobContextImpl env = clientEnvs.get(client);
		if (env == null) {
			if (log.isDebugEnabled())
				log.debug("Creating new client environment for client " + client);
			env = new JobContextImpl(srv, new ClientCluster(srv, client), client);
			clientEnvs.put(client, env);
		}
		return env;
	}

	public void remove(Peer srv) {
		JobContext cliEnv = clientEnvs.remove(srv.getAddress());
		if (cliEnv != null)
			cliEnv.stop();
	}

	public void stop() {
		for (Entry<Peer, JobContextImpl> e : clientEnvs.entrySet()) {
			e.getValue().stop();
		}

		clientEnvs.clear();

	}
}
