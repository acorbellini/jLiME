package edu.jlime.jd.client;

import edu.jlime.jd.ClientCluster;

public interface JobContext {

	public abstract boolean contains(String map);

	public abstract Object get(String prop);

	public abstract ClientCluster getCluster();

	public abstract void put(String prop, Object val);

	public abstract void putIfAbsent(String hashName, Object val);

	public abstract Object remove(String prop);

	public abstract void stop();

	public abstract Object waitFor(String id);

}