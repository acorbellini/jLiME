package edu.jlime.jd.task;

public abstract interface ResultListener<T, R> {

	public void onSuccess(T result) throws Exception;

	public R onFinished();

	public void onFailure(Exception res);
}