package edu.jlime.jd.task;

public interface Task<T> {
	public <R> R execute(final ResultListener<T, R> listener) throws Exception;

	public abstract <R> R execute(int maxjobs, ResultListener<T, R> listener) throws Exception;

}