package edu.jlime.jd.task;

public interface Task<T> {

	public abstract <R> R execute(ResultListener<T, R> listener);

}