package edu.jlime.jd.client;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.JobContainer;

public interface JobContext {

	public abstract boolean contains(String map);

	public abstract Object get(String prop);

	public abstract ClientCluster getCluster();

	public abstract void put(String prop, Object val);

	public abstract void putIfAbsent(String hashName, Object val);

	public abstract Object remove(String prop);

	public abstract void stop();

	public abstract Object waitFor(String id);

	public abstract Object getGlobal(String k);

	public abstract void execute(JobContainer j) throws Exception;

}