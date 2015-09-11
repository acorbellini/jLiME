package edu.jlime.metrics.sysinfo.linux;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxOsInfo extends SysInfoProvider {

	@Override
	public void load(Metrics mgr) throws Exception {
		String uname = CommandLineUtils.execCommand("uname -snm").trim();
		String[] generalData = uname.split(" ");
		mgr.simple("sysinfo.os.type", generalData[0].trim());
		mgr.simple("sysinfo.os.type.hostname", generalData[1].trim());
		mgr.simple("sysinfo.os.type.arch", generalData[2].trim());
		mgr.simple("sysinfo.os.type.dist", CommandLineUtils.execCommand("uname -v").trim());
	}

}
