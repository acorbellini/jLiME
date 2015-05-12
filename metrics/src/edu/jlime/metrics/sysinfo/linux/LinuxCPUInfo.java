package edu.jlime.metrics.sysinfo.linux;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxCPUInfo extends SysInfoProvider {
	File f = new File("/proc/stat");

	boolean first = true;
	long prevIdle = -1;
	long prevNonIdle = -1;

	public LinuxCPUInfo() throws FileNotFoundException {

	}

	@Override
	public void load(final Metrics mgr) {
		try {
			final String cores = CommandLineUtils.execCommand(
					"cat /proc/cpuinfo | grep -c processor | tr -d '\\n'")
					.trim();
			mgr.simple("sysinfo.cpu.cores", cores);
			mgr.simple(
					"sysinfo.cpu.cpuname",
					CommandLineUtils
							.execCommand(
									"cat /proc/cpuinfo | grep 'model name'|uniq| tr -s ' ' | tr -d '\t'")
							.substring(12).trim());
			mgr.simple(
					"sysinfo.cpu.freq",
					CommandLineUtils
							.execCommand(
									"lscpu | tr -d ' ' | grep 'CPUMHz' | cut -d':' -f2")
							.trim());
			mgr.createTimedSensor(new SensorMeasure() {
				@Override
				public void proc(Metrics mgr) throws Exception {
					mgr.gauge("sysinfo.cpu.usage").update(getCpuUsage(cores));
				}
			});

		} catch (Exception e) {
			Logger.getLogger(LinuxCPUInfo.class).error(
					"Error executing command in linux cpu info.", e);
		}
	}

	private Float getCpuUsage(final String cores) throws Exception {

		BufferedReader reader = new BufferedReader(new FileReader(f));
		String[] cpuStats = reader.readLine().replaceAll("\\s", " ").split(" ");
		reader.close();
		long idle = Integer.valueOf(cpuStats[4]) + Integer.valueOf(cpuStats[5]);
		long nonidle = Integer.valueOf(cpuStats[1])
				+ Integer.valueOf(cpuStats[2]) + Integer.valueOf(cpuStats[3])
				+ Integer.valueOf(cpuStats[6]) + Integer.valueOf(cpuStats[7])
				+ Integer.valueOf(cpuStats[8]);

		long prevtotal = prevIdle + prevNonIdle;
		long total = idle + nonidle;

		float usage = ((total - prevtotal) - (idle - prevIdle))
				/ (float) (total - prevtotal);

		prevIdle = idle;
		prevNonIdle = nonidle;

		if (first) {
			first = false;
			return 0f;
		}
		return usage;
	}
}
