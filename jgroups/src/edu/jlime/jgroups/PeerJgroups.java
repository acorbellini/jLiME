package edu.jlime.jgroups;

import java.util.Arrays;

import org.jgroups.Address;

import edu.jlime.core.cluster.IP;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.ServerAddressParser;
import edu.jlime.jd.JobDispatcher;

public class PeerJgroups extends Peer {

	private static final long serialVersionUID = 5947505275501091253L;

	public static PeerJgroups createNew(Address raw, JobDispatcher disp)
			throws Exception {
		ServerAddressParser parser = new ServerAddressParser(raw.toString());
		PeerJgroups pjg = new PeerJgroups(parser.getId(), parser.getIp(), raw);
		pjg.putData(JobDispatcher.ISEXEC, Boolean.valueOf(parser.isExec())
				.toString());
		pjg.putData(JobDispatcher.TAGS, Arrays.toString(parser.getTags()));
		return pjg;
	}

	public PeerJgroups(String id, IP ip, Address address) {
		super(new JGroupsAddress(address), ip.toString());
	}

}
