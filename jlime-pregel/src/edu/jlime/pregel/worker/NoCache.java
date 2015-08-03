package edu.jlime.pregel.worker;

public class NoCache implements CacheManagerI {

	private WorkerTask task;

	public NoCache(WorkerTask workerTask) {
		this.task = workerTask;
	}

	@Override
	public void flush() throws Exception {
	}

	@Override
	public void send(String msgtype, long from, long to, Object val)
			throws Exception {
		task.outputObject(msgtype, from, to, val);
	}

	@Override
	public void sendAll(String msgType, long from, Object val) throws Exception {
		task.outputObject(msgType, from, -1, val);
	}

	@Override
	public void sendFloat(String msgtype, long from, long to, float val)
			throws Exception {
		task.outputFloat(msgtype, from, to, val);
	}

	@Override
	public void sendAllFloat(String msgType, long from, float val)
			throws Exception {
		task.outputFloat(msgType, from, -1, val);
	}

	@Override
	public void sendAllDouble(String msgType, long from, double val)
			throws Exception {
		task.outputDouble(msgType, from, -1, val);
	}

	@Override
	public void sendDouble(String msgType, long from, long to, double val)
			throws Exception {
		task.outputDouble(msgType, from, to, val);
	}

}
