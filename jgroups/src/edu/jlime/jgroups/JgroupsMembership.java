package edu.jlime.jgroups;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgroups.Address;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.DiscoveryListener;
import edu.jlime.core.transport.DiscoveryProvider;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;

public class JgroupsMembership implements DiscoveryProvider, FailureProvider {

	HashMap<Address, edu.jlime.core.transport.Address> addressToPeer = new HashMap<>();
	HashMap<edu.jlime.core.transport.Address, Address> peerToAddress = new HashMap<>();

	private FailureListener fail;
	private DiscoveryListener disco;

	@Override
	public void addListener(FailureListener l) {
		this.fail = l;
	}

	@Override
	public void addPeerToMonitor(Peer peer) throws Exception {
	}

	@Override
	public void addListener(DiscoveryListener l) {
		this.disco = l;

	}

	@Override
	public void putData(Map<String, String> dataMap) {
	}

	public void nodeDeleted(edu.jlime.core.transport.Address peer) {
		fail.nodeFailed(peer);
	}

	public void nodeAdded(edu.jlime.core.transport.Address addr, String name,
			Map<String, String> data) throws Exception {
		disco.memberMessage(addr, name, data, new Object());
	}

	public void update(List<Address> newList) throws Exception {
		Set<Address> current = addressToPeer.keySet();

		HashSet<Address> added = new HashSet<>(newList);
		HashSet<Address> removed = new HashSet<>(current);

		added.removeAll(current);
		removed.retainAll(newList);

		for (Address address : added) {
			edu.jlime.core.transport.Address value = new edu.jlime.core.transport.Address();
			addressToPeer.put(address, value);
			peerToAddress.put(value, address);

			String name = getName(address);
			HashMap<String, String> data = getData(address);
			nodeAdded(value, name, data);
		}

		for (Address address : removed) {
			edu.jlime.core.transport.Address peer = addressToPeer.get(address);
			addressToPeer.remove(address);
			peerToAddress.remove(peer);
			nodeDeleted(peer);
		}

	}

	private HashMap<String, String> getData(Address address) {
		try {
			HashMap<String, String> ret = new HashMap<>();
			String[] split = address.toString().split("/");
			String[] data = split[1].split(",");
			for (String d : data) {
				String[] kv = d.split("=");
				ret.put(kv[0], kv[1]);
			}
			return ret;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new HashMap<>();
		}
	}

	private String getName(Address address) {
		return address.toString().split("/")[0];
	}

	public Address getJgroupsAddress(edu.jlime.core.transport.Address address) {
		return peerToAddress.get(address);
	}

	public edu.jlime.core.transport.Address getAddress(Address origin) {
		return addressToPeer.get(origin);
	}

	@Override
	public Object getAddresses() {
		return null;
	}

}
