package edu.jlime.jd.profiler;

import java.util.Date;
import java.util.TreeMap;

public interface ProfilerFunctionPerNode<T> {
	public T call(TreeMap<Date, T> value);
}
