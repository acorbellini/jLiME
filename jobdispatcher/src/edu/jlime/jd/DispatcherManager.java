package edu.jlime.jd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DispatcherManager {

	private static Map<String, JobDispatcher> localDispatchers = new ConcurrentHashMap<String, JobDispatcher>();

	public static void registerJD(JobDispatcher jobDispatcher) {
		localDispatchers.put(jobDispatcher.getID(), jobDispatcher);
	}

	public static void unregisterJD(JobDispatcher jobDispatcher) {
		localDispatchers.remove(jobDispatcher.getID());
	}

	public static JobExecutor getJD(String id) {
		return localDispatchers.get(id);
	}

}
