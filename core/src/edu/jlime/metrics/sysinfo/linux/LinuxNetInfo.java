package edu.jlime.metrics.sysinfo.linux;

import java.util.HashSet;
import java.util.Set;

import edu.jlime.metrics.metric.IMetrics;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.TimedSet;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxNetInfo extends SysInfoProvider {

	@Override
	public void load(Metrics mgr) {
		mgr.createTimedSensor(new TimedSet() {
			@Override
			public Set<String> updateSet(IMetrics mgr) throws Exception {
				HashSet<String> currentifaces = new HashSet<>();
				String[] ifaces = CommandLineUtils
						.execCommand(
								"ifconfig | cut -d' ' -f1 | tr '\\n' ' '| tr -s ' '")
						.trim().split(" ");
				for (String iface : ifaces) {
					String currentRoot = "sysinfo.net." + iface;
					currentifaces.add(currentRoot);

					mgr.simple(currentRoot + ".ifname", iface);

					String ifconfigForIface = CommandLineUtils
							.execCommand("ifconfig "
									+ iface
									+ " | grep -o -E [0-9]+\\\\.[0-9]+\\\\.[0-9]+\\\\.[0-9]+ | tr '\\n' ' '| tr -s ' '");
					if (!ifconfigForIface.isEmpty()) {
						String[] ipdata = ifconfigForIface.trim().split(" ");
						mgr.simple(currentRoot + ".ip", ipdata[0]);
						mgr.simple(currentRoot + ".broadcast", ipdata[1]);
					}

					String ifconfigConsumption = CommandLineUtils
							.execCommand("cat /sys/class/net/" + iface
									+ "/statistics/rx_bytes /sys/class/net/"
									+ iface + "/statistics/tx_bytes");
					String[] bytes = ifconfigConsumption.replace("\n", " ")
							.split(" ");
					// if (log.isDebugEnabled())
					// log.debug("Consumption info (Ifconfig) result of interface "
					// + iface + " : " + ifconfigConsumption);

					Float rcvd = Float.valueOf(bytes[0]) / 1024;
					Float sent = Float.valueOf(bytes[1]) / 1024;

					mgr.meter(currentRoot + ".recv_total").update(rcvd);
					mgr.meter(currentRoot + ".sent_total").update(sent);
				}

				return currentifaces;
			}
		});
	}

}
