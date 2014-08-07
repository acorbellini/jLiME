package edu.jlime.jd.task;

public abstract interface ResultListener<R> {

	public void onSuccess(R result);

	public void onFinished();

	public void onFailure(Exception res);
}