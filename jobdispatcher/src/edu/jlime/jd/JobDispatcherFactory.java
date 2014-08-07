package edu.jlime.jd;

public abstract class JobDispatcherFactory {

	public abstract JobDispatcher getJD() throws Exception;

	// public static JobDispatcherFactory getJGroupsFactory(InputStream jg,
	// int minPeers, String[] tags, boolean isExec) {
	// return new JGroupsFactory(jg, minPeers, tags, isExec);
	// }

	// public static JobDispatcherFactory getDEFFactory(int minPeers,
	// String[] tags, boolean isExec, Properties defConfig)
	// throws Exception {
	// return new DEFFactory(minPeers, tags, isExec, defConfig);
	// }
}
