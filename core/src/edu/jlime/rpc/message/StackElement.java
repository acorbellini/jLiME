package edu.jlime.rpc.message;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;

public interface StackElement {

	public void start() throws Exception;

	public void cleanupOnFailedPeer(Address address);

	public void stop() throws Exception;

	public void setMetrics(Metrics metrics);
}