package edu.jlime.metrics.sysinfo.linux;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import edu.jlime.metrics.metric.IMetrics;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.TimedSet;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxIoStatInfo extends SysInfoProvider {

	@Override
	public void load(Metrics mgr) {

		mgr.createTimedSensor(new TimedSet() {

			@Override
			public Set<String> updateSet(IMetrics mgr) throws Exception {
				Set<String> current = new HashSet<>();

				String iorawdata = CommandLineUtils.execCommand(
						"iostat -p -d | tail -n+4 | tr -s ' '").trim();
				BufferedReader reader = new BufferedReader(new StringReader(
						iorawdata));

				String line = null;
				while ((line = reader.readLine()) != null) {
					String[] iodata = line.split(" ");
					String k = "sysinfo.iostat." + iodata[0];

					mgr.simple(k, iodata[0]);

					mgr.gauge(k + ".tps").update(
							Float.valueOf(iodata[1].replaceAll(",", ".")));
					mgr.gauge(k + ".read").update(
							Float.valueOf(iodata[2].replaceAll(",", ".")));
					mgr.gauge(k + ".write").update(
							Float.valueOf(iodata[3].replaceAll(",", ".")));
					mgr.gauge(k + ".read_total").update(
							Float.valueOf(iodata[4].replaceAll(",", ".")));
					mgr.gauge(k + ".write_total").update(
							Float.valueOf(iodata[5].replaceAll(",", ".")));
					current.add(k);
				}
				return current;

			}
		});

	}
}
