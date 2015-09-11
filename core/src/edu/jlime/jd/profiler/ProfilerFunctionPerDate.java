package edu.jlime.jd.profiler;

import java.util.TreeMap;

import edu.jlime.jd.Node;

public interface ProfilerFunctionPerDate<T> {

	T call(TreeMap<Node, T> value);

}
