package edu.jlime.rpc.discovery;

import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class MultiCastDiscovery extends Discovery {

	Thread t;

	protected boolean stopped;

	Logger log = Logger.getLogger(MultiCastDiscovery.class);

	// ,int announcedPort, String mcastaddr,
	// int mcastport, long discDelay

	public MultiCastDiscovery(UUID id, Configuration config,
			MessageProcessor mcast, MessageProcessor unicast) {
		super(id, config, mcast, unicast);
	}

	protected synchronized void startDiscovery(List<SelectedInterface> added) {
		if (t != null)
			return;
		t = new Thread("Multicast Discovery Thread") {
			public void run() {
				int times = 0;
				while (!stopped && times < config.disc_num_tries) {
					try {
						discoveryInit.queue(newDiscoveryMessage());
					} catch (Exception e) {
						log.error("Could not send discovery message.");
					}
					try {
						Thread.sleep(config.disc_delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					times++;
				}
				t = null;
			}

		};
		t.start();
	}

	@Override
	public void cleanupOnFailedPeer(Address peer) {

	}

	@Override
	public void stop() throws Exception {
		stopped = true;
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
