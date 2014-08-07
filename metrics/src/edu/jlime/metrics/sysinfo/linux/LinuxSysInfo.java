package edu.jlime.metrics.sysinfo.linux;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.metrics.sysinfo.InfoProvider;

public class LinuxSysInfo {

	public static List<InfoProvider> getProviders() {
		ArrayList<InfoProvider> list = new ArrayList<>();
		list.add(new LinuxMemoryInfo());
		list.add(new LinuxCPUInfo());
		list.add(new LinuxOsInfo());
		list.add(new LinuxNetInfo());
		list.add(new LinuxDiskInfo());
		list.add(new LinuxIoStatInfo());
		return list;
	}

}
