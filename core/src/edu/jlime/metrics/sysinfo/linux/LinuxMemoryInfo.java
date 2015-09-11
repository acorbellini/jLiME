package edu.jlime.metrics.sysinfo.linux;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxMemoryInfo extends SysInfoProvider {

	Logger log = Logger.getLogger(LinuxMemoryInfo.class);

	@Override
	public void load(Metrics mgr) {
		mgr.createTimedSensor(new SensorMeasure() {

			@Override
			public void proc(Metrics mgr) throws Exception {
				String[] free = CommandLineUtils.execCommand("free | grep Mem | tr -s ' '").trim().split("\\s");
				String root = "sysinfo.mem";
				mgr.gauge(root + ".total").update(Float.valueOf(free[1]));
				mgr.gauge(root + ".used").update(Float.valueOf(free[2]));
				mgr.gauge(root + ".available").update(Float.valueOf(free[3]));
				mgr.gauge(root + ".buffers").update(Float.valueOf(free[5]));
				mgr.gauge(root + ".cached").update(Float.valueOf(free[6]));
			}
		});
	}
}
