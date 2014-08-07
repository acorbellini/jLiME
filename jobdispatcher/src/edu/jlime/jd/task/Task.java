package edu.jlime.jd.task;

import edu.jlime.jd.job.Job;

public interface Task<R> {

	public abstract <J extends Job<R>> void execute(ResultListener<R> listener);

}