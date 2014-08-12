package edu.jlime.jd.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;

public class ForkJoinTask<T> extends TaskBase<T> {

	Map<Job<T>, JobNode> map = new ConcurrentHashMap<Job<T>, JobNode>();

	public ForkJoinTask() {
	}

	public void putJob(Job<T> j, JobNode p) {
		map.put(j, p);
	}

	@Override
	protected Map<Job<T>, JobNode> getMap() {
		return map;
	}
}
