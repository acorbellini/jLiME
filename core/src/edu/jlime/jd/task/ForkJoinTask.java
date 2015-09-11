package edu.jlime.jd.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.jd.Node;
import edu.jlime.jd.job.Job;

public class ForkJoinTask<T> extends TaskBase<T> {

	Map<Job<T>, Node> map = new ConcurrentHashMap<Job<T>, Node>();

	public ForkJoinTask() {
	}

	public void putJob(Job<T> j, Node p) {
		map.put(j, p);
	}

	@Override
	public Map<Job<T>, Node> getMap() {
		return map;
	}
}
