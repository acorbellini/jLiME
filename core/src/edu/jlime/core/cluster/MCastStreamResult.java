package edu.jlime.core.cluster;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.core.stream.RemoteInputStream;

public class MCastStreamResult {

	BroadcastOutputStream os;

	List<RemoteInputStream> inputs = new ArrayList<>();

	public MCastStreamResult(BroadcastOutputStream os) {
		this.os = os;
	}

	public void addInput(RemoteInputStream input) {
		inputs.add(input);
	}
}
