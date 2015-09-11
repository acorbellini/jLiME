package edu.jlime.metrics.sysinfo;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.jvm.JVMInfoProvider;
import edu.jlime.metrics.sysinfo.jvm.JVMMemoryProvider;
import edu.jlime.metrics.sysinfo.linux.LinuxSysInfo;

public abstract class SysInfoProvider extends InfoProvider {

	public static List<InfoProvider> get() {
		String os = System.getProperty("os.name");
		// if (os.contains("win"))
		// provider = new WindowsSysInfo();
		List<InfoProvider> osProviders = new ArrayList<>();
		if (os.contains("nix") || os.contains("nux") || os.contains("aix"))
			osProviders.addAll(LinuxSysInfo.getProviders());
		else if (os.contains("Windows")) {
			osProviders.add(new InfoProvider() {
				float cont = 0f;

				@Override
				public void load(Metrics mgr) throws Exception {
					mgr.createTimedSensor(new SensorMeasure() {

						@Override
						public void proc(Metrics mgr) throws Exception {
							mgr.meter("sysinfo.net.eth0.sent_total").update(cont);
							cont += 100f;
						}
					});
				}
			});
		}

		osProviders.add(new JVMInfoProvider());
		osProviders.add(new JVMMemoryProvider());
		return osProviders;
	}

	public static Long totalnet(String iFaceName, CompositeMetrics<?> info) throws NumberFormatException {
		Long total = 0l;
		for (Metrics mgr : info.getMap().values()) {
			Metric<?> ifdata = mgr.get("sysinfo.net." + iFaceName + ".sent_total");
			total += Long.valueOf(ifdata.toString());
		}
		return total;
	}

}