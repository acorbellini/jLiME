package edu.jlime.jd.profiler;

import java.util.Date;
import java.util.Map;

import edu.jlime.jd.Node;

public interface Profiler {

	public abstract void start();

	public abstract void stop();

	public abstract <T> Map<Node, T> calcPerNode(ProfilerFunctionPerNode<T> profilerFunction, MetricExtractor<T> ext);

	public abstract <T> Map<Date, T> calcPerDate(ProfilerFunctionPerDate<T> profilerFunction, MetricExtractor<T> ext);

}