package edu.jlime.collections.adjacencygraph.mappers;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.JobNode;
import edu.jlime.metrics.sysinfo.filter.LogFilter;
import edu.jlime.metrics.sysinfo.filter.SimpleInfoFilter;

public class MapperFactory {

	public static Mapper cpuCriteria() {
		return new CriteriaMapper(new LogFilter<JobNode>(new SimpleInfoFilter<JobNode>(
				"cpu.usage"), false));
	}

	public static Mapper memCriteria() {
		return new CriteriaMapper(new SimpleInfoFilter<JobNode>("jvm_memory.max"));
	}
}