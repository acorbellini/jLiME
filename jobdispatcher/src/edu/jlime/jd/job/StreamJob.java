package edu.jlime.jd.job;

import java.util.UUID;

import edu.jlime.client.JobContext;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.ClientNode;

public abstract class StreamJob extends RunJob {

	private UUID streamIDInput;

	private UUID streamIDOutput;

	private boolean finished;

	public StreamJob() {
		this.streamIDInput = UUID.randomUUID();
		this.streamIDOutput = UUID.randomUUID();
	}

	public final void run(JobContext env, ClientNode origin) throws Exception {
		run(env.getCluster().getInputStream(streamIDInput, origin), env
				.getCluster().getOutputStream(streamIDOutput, origin), env);
	}

	public abstract void run(RemoteInputStream inputStream,
			RemoteOutputStream outputStream, JobContext ctx) throws Exception;

	public UUID getStreamIDInput() {
		return streamIDInput;
	}

	public UUID getStreamIDOutput() {
		return streamIDOutput;
	}

	public synchronized void setFinished(boolean b) {
		finished = true;
		notifyAll();
	}

	public synchronized void waitForFinished() {
		while (!finished)
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}
}
