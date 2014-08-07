package edu.jlime.jd.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;

public class ForkJoinTask<R> extends TaskBase<R> {

	Map<Job<R>, JobNode> map = new ConcurrentHashMap<Job<R>, JobNode>();

	public ForkJoinTask() {
	}

	public void putJob(Job<R> j, JobNode p) {
		map.put(j, p);
	}

	@Override
	protected Map<Job<R>, JobNode> getMap() {
		return map;
	}
}
