package edu.jlime.jd.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;

public class ForkJoinTask<T> extends TaskBase<T> {

	Map<Job<T>, ClientNode> map = new ConcurrentHashMap<Job<T>, ClientNode>();

	public ForkJoinTask() {
	}

	public void putJob(Job<T> j, ClientNode p) {
		map.put(j, p);
	}

	@Override
	protected Map<Job<T>, ClientNode> getMap() {
		return map;
	}
}
