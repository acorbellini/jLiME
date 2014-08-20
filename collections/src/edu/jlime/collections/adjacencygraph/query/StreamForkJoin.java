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
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.StreamJob;

public abstract class StreamForkJoin {

	public static interface StreamJobFactory {

		public StreamJob getStreamJob();
	}

	public void execute(List<ClientNode> peers, StreamJobFactory factory)
			throws Exception {
		ExecutorService execOutput = Executors.newCachedThreadPool();
		ExecutorService execInput = Executors.newCachedThreadPool();

		List<InputStream> input = new ArrayList<InputStream>();
		for (final ClientNode p : peers) {
			final StreamResult res = p.stream(factory.getStreamJob());
			input.add(res.getIs());
			execOutput.execute(new Runnable() {

				@Override
				public void run() {
					send(res.getOs(), p);
				}
			});
			execInput.execute(new Runnable() {

				@Override
				public void run() {
					receive(res.getIs(), p);
				}
			});
		}
		execOutput.shutdown();
		execInput.shutdown();

		execOutput.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		execInput.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	protected abstract void send(RemoteOutputStream os, ClientNode p);

	protected abstract void receive(RemoteInputStream is, ClientNode p);

}
