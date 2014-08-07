package edu.jlime.metrics.sysinfo.jvm;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;

public class JVMInfoProvider extends InfoProvider {

	@Override
	public void load(Metrics mgr) throws Exception {
		mgr.simple("sysinfo.os", System.getProperty("os.name"));
	}

}
