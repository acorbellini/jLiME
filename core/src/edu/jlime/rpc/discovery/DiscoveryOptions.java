package edu.jlime.rpc.discovery;

public class DiscoveryOptions {

	long discDelay = 1500;

	int max_times = 10;

	public DiscoveryOptions(long discDelay, int max_times) {
		super();
		this.discDelay = discDelay;
		this.max_times = max_times;
	}

}