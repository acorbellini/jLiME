package edu.jlime.rpc.discovery;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class MultiCastDiscovery extends Discovery {

	Thread t;

	protected boolean stopped;

	Logger log = Logger.getLogger(MultiCastDiscovery.class);

	// ,int announcedPort, String mcastaddr,
	// int mcastport, long discDelay

	public MultiCastDiscovery(Address id, String name, Configuration config,
			MessageProcessor mcast, MessageProcessor unicast) {
		super(id, name, config, mcast, unicast);
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
			}
		};
		t.start();
	}

	@Override
	public void stop() throws Exception {
		stopped = true;
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
