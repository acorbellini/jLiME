package edu.jlime.graphly.jobs;

import edu.jlime.jd.ClientNode;
import edu.jlime.metrics.sysinfo.filter.LogFilter;
import edu.jlime.metrics.sysinfo.filter.SimpleInfoFilter;

public class MapperFactory {

	public static Mapper cpuCriteria() {
		return new CriteriaMapper(new LogFilter<ClientNode>(
				new SimpleInfoFilter<ClientNode>("sysinfo.cpu.usage"), false));
	}

	public static Mapper simple(String criteria) {
		return new CriteriaMapper(new SimpleInfoFilter<ClientNode>(criteria));
	}

	public static Mapper location() {
		return new LocationMapper();
	}

	public static Mapper rr() {
		return new RoundRobinMapper();
	}
}