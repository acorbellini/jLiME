package edu.jlime.jgroups;

import org.jgroups.Address;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.ServerAddressParser;
import edu.jlime.jd.JobDispatcher;

public class PeerJgroups {

	private PeerJgroups() {
	}

	public static Peer createNew(Address raw) throws Exception {
		ServerAddressParser parser = new ServerAddressParser(raw.toString());
		return new Peer(new JGroupsAddress(raw), parser.getIp().toString());
	}

}
