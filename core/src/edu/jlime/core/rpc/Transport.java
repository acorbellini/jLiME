package edu.jlime.core.rpc;

import edu.jlime.core.cluster.Peer;
import edu.jlime.metrics.metric.Metrics;

public interface Transport {

	public void sendAsync(Peer p, byte[] marshalled) throws Exception;

	public byte[] sendSync(Peer p, byte[] marshalled) throws Exception;

	public void registerReceiver(DataReceiver rcvr);

	public void start() throws Exception;

	public void stop();

	public void setMetrics(Metrics metrics);
}
