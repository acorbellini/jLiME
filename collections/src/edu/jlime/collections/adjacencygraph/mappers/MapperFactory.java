package edu.jlime.collections.adjacencygraph.mappers;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.metrics.sysinfo.filter.LogFilter;
import edu.jlime.metrics.sysinfo.filter.SimpleInfoFilter;

public class MapperFactory {

	public static Mapper cpuCriteria() {
		return new CriteriaMapper(new LogFilter<ClientNode>(new SimpleInfoFilter<ClientNode>(
				"sysinfo.cpu.usage"), false));
	}

	public static Mapper memCriteria() {
		return new CriteriaMapper(new SimpleInfoFilter<ClientNode>("jvminfo.mem.max"));
	}
}