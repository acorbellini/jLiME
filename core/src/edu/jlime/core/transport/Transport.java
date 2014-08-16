package edu.jlime.core.transport;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.DataReceiver;
import edu.jlime.metrics.metric.Metrics;

public interface Transport {

	public void sendAsync(Peer p, byte[] marshalled) throws Exception;

	public byte[] sendSync(Peer p, byte[] marshalled) throws Exception;

	public void registerReceiver(DataReceiver rcvr);

	public void start() throws Exception;

	public void stop() throws Exception;

	public void setMetrics(Metrics metrics);

	public Streamer getStreamer();
}
