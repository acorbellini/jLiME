package edu.jlime.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.discovery.Discovery;
import edu.jlime.rpc.discovery.DiscoveryListener;
import edu.jlime.rpc.discovery.DiscoveryProvider;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;

public class MultiDiscovery extends MessageProcessor implements
		DiscoveryProvider {

	public MultiDiscovery() {
		super("MultiDiscovery");
	}

	List<Discovery> discos = new ArrayList<>();

	public void addDisco(Discovery disco) {
		this.discos.add(disco);
	}

	@Override
	public void start() throws Exception {
		for (Discovery d : discos)
			d.start();
	}

	@Override
	public void addListener(DiscoveryListener l) {
		for (Discovery d : discos)
			d.addListener(l);
	}

	@Override
	public void send(Message msg) throws Exception {
	}

	@Override
	public void putData(HashMap<String, String> dataMap) {
		for (Discovery d : discos)
			d.putData(dataMap);

	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
