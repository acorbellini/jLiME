package edu.jlime.jd.profiler;

import java.util.TreeMap;

import edu.jlime.jd.ClientNode;

public interface ProfilerFunctionPerDate<T> {

	T call(TreeMap<ClientNode, T> value);

}
