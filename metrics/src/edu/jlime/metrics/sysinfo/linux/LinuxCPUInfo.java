package edu.jlime.metrics.sysinfo.linux;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CommandLineUtils;

public class LinuxCPUInfo extends SysInfoProvider {

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
					mgr.gauge("sysinfo.cpu.freq").update(getCpuUsage(cores));
				}
			});

		} catch (Exception e) {
			Logger.getLogger(LinuxCPUInfo.class).error(
					"Error executing command in linux cpu info.", e);
		}
	}

	private Float getCpuUsage(final String cores) throws Exception {
		Integer cpuusagepos = Integer
				.valueOf(CommandLineUtils
						.execCommand(
								"top -bn 1 | tail -n+7 | head -1 | tr -s ' ' | sed \"s/^ *//g\" | tr ' ' '\n' | cat -n | grep %CPU | cut -f1")
						.trim());

		String usage = CommandLineUtils.execCommand(
				" echo \"(\"$(top -bn 1 | tail -n+8 "
						+ "| tr -s ' ' | sed \"s/^ *//g\" | cut -d' ' -f"
						+ cpuusagepos
						+ " | tr , . | paste -s -d+ - | sed \"s/+*$//g\" )\")/"
						+ cores + "\"|bc").trim();
		return Float.valueOf(usage);
	}
}
