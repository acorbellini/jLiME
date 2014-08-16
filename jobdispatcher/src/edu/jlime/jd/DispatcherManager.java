package edu.jlime.jd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;

public class DispatcherManager {

	private static Map<Peer, JobDispatcher> localDispatchers = new ConcurrentHashMap<>();

	public static void registerJD(JobDispatcher jobDispatcher) {
		localDispatchers.put(jobDispatcher.getLocalPeer(), jobDispatcher);
	}

	public static void unregisterJD(JobDispatcher jobDispatcher) {
		localDispatchers.remove(jobDispatcher.getLocalPeer());
	}

	public static JobExecutor getJD(Address id) {
		return localDispatchers.get(id);
	}

}
