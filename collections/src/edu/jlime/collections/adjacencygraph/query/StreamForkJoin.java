package edu.jlime.collections.adjacencygraph.query;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;

public abstract class StreamForkJoin {

	public static interface StreamJobFactory {

		public StreamJob getStreamJob();
	}

	public void execute(List<JobNode> peers, StreamJobFactory factory)
			throws Exception {
		ExecutorService execOutput = Executors.newCachedThreadPool();
		ExecutorService execInput = Executors.newCachedThreadPool();

		List<InputStream> input = new ArrayList<InputStream>();
		for (final JobNode p : peers) {
			final StreamResult res = p.stream(factory.getStreamJob());
			input.add(res.getIs());
			execOutput.execute(new Runnable() {

				@Override
				public void run() {
					sendOutput(res.getOs(), p);
				}
			});
			execInput.execute(new Runnable() {

				@Override
				public void run() {
					receiveInput(res.getIs(), p);
				}
			});
		}
		execOutput.shutdown();
		execInput.shutdown();

		execOutput.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		execInput.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	protected abstract void sendOutput(RemoteOutputStream os, JobNode p);

	protected abstract void receiveInput(RemoteInputStream is, JobNode p);

}
