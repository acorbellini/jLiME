package edu.jlime.jd.task;

public abstract interface ResultListener<T, R> {

	public void onSuccess(T result);

	public R onFinished();

	public void onFailure(Exception res);
}