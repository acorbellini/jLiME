package edu.jlime.metrics.sysinfo.jvm;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.InfoProvider;

public class JVMMemoryProvider extends InfoProvider {

	private Runtime rt;

	private long max;

	private int mb = 1024 * 1024;

	public JVMMemoryProvider() {
		rt = Runtime.getRuntime();
		max = rt.maxMemory() / mb;
	}

	@Override
	public void load(Metrics mgr) throws Exception {
		mgr.createTimedSensor(new SensorMeasure() {

			@Override
			public void proc(Metrics mgr) throws Exception {
				float free = (rt.freeMemory() / mb);
				float total = rt.totalMemory() / mb;
				mgr.gauge("jvminfo.mem.total").update(total);
				mgr.gauge("jvminfo.mem.used").update(total - free);
				mgr.gauge("jvminfo.mem.free").update(free);
				mgr.simple("jvminfo.mem.max").update(max);
			}
		});

	}
}
