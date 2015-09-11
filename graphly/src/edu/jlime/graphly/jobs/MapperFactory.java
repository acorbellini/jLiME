package edu.jlime.graphly.jobs;

import edu.jlime.jd.Node;
import edu.jlime.metrics.sysinfo.filter.LogFilter;
import edu.jlime.metrics.sysinfo.filter.SimpleInfoFilter;

public class MapperFactory {

	public static Mapper cpuCriteria() {
		return new CriteriaMapper(new LogFilter<Node>(new SimpleInfoFilter<Node>("sysinfo.cpu.usage"), false), true);
	}

	public static Mapper simple(String criteria, Boolean dynamic) {
		return new CriteriaMapper(new SimpleInfoFilter<Node>(criteria), dynamic);
	}

	public static Mapper location() {
		return new LocationMapper();
	}

	public static Mapper rr() {
		return new RoundRobinMapper();
	}

	public static Mapper hybrid(float[] divs, Mapper... mappers) {
		return new HybridMapper(mappers, divs);
	}
}