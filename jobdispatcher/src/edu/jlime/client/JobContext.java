package edu.jlime.client;

import edu.jlime.jd.JobCluster;

public interface JobContext {

	public abstract boolean contains(String map);

	public abstract Object get(String prop);

	public abstract JobCluster getCluster();

	public abstract void put(String prop, Object val);

	public abstract void putIfAbsent(String hashName, Object val);

	public abstract Object remove(String prop);

	public abstract void stop();

	public abstract Object waitFor(String id);

}