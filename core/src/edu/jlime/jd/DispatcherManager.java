package edu.jlime.jd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.rpc.JobExecutor;

public class DispatcherManager {

	private static Map<Peer, Dispatcher> localDispatchers = new ConcurrentHashMap<>();

	public static void registerJD(Dispatcher jobDispatcher) {
		localDispatchers.put(jobDispatcher.getLocalPeer(), jobDispatcher);
	}

	public static void unregisterJD(Dispatcher jobDispatcher) {
		localDispatchers.remove(jobDispatcher.getLocalPeer());
	}

	public static JobExecutor getJD(Peer id) {
		return localDispatchers.get(id);
	}

}
