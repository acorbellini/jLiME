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

public class LinuxDiskInfo extends SysInfoProvider {

	@Override
	public void load(Metrics mgr) {
		mgr.createTimedSensor(new TimedSet() {
			@Override
			public Set<String> updateSet(IMetrics mgr) throws Exception {
				HashSet<String> currentDevs = new HashSet<>();
				String df = CommandLineUtils.execCommand(
						"df -t ext4 -t ext3 -t ext2 -t vfat -t ntfs -t ntfs-3g -T  | tail -n +2 | tr -s ' '");
				StringReader reader = new StringReader(df);
				BufferedReader buf = new BufferedReader(reader);
				String line;
				while ((line = buf.readLine()) != null) {
					String[] diskData = line.split(" ");
					String dev = diskData[0].replaceAll("/dev/", "");
					String deviceMetric = "sysinfo.disk." + dev;
					mgr.simple(deviceMetric, dev);
					mgr.simple(deviceMetric + ".type", diskData[2]);
					mgr.gauge(deviceMetric + ".used").update(Float.valueOf(diskData[3]));
					mgr.gauge(deviceMetric + ".available").update(Float.valueOf(diskData[4]));
					mgr.simple(deviceMetric + ".diskname", diskData[6]);

					currentDevs.add(deviceMetric);
				}
				return currentDevs;
			}
		});
	}
}
