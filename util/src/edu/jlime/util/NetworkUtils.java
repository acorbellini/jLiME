package edu.jlime.util;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

public class NetworkUtils {

	static Logger log = Logger.getLogger(NetworkUtils.class);

	private static List<NetworkChangeListener> listeners = Collections
			.synchronizedList(new ArrayList<NetworkChangeListener>());

	static List<SelectedInterface> last;
	static {
		Thread t = new Thread() {

			@Override
			public void run() {
				while (true) {
					List<SelectedInterface> local = getLocalAddress(false);

					if (last == null) {
						last = local;
						return;
					}
					// if (log.isDebugEnabled()) {
					// log.debug("Last Interfaces : " + last);
					// log.debug("Current interfaces: " + local);
					//
					// }
					//

					List<SelectedInterface> added = new ArrayList<>(local);
					List<SelectedInterface> removed = new ArrayList<>(last);
					for (Iterator<SelectedInterface> currListIt = added
							.iterator(); currListIt.hasNext();) {
						SelectedInterface currIface = currListIt.next();
						for (Iterator<SelectedInterface> lastListIt = removed
								.iterator(); lastListIt.hasNext();) {
							SelectedInterface lastIface = lastListIt.next();
							if (currIface.getNif().getName()
									.equals(lastIface.getNif().getName())) {
								lastListIt.remove();
								currListIt.remove();
								break;
							}
						}
					}
					last = local;
					if (added.isEmpty() && removed.isEmpty())
						return;
					if (log.isDebugEnabled()) {
						if (!added.isEmpty())
							log.debug("New interfaces added : " + added);
						if (!removed.isEmpty())
							log.debug("Interfaces removed : " + removed);
					}

					for (NetworkChangeListener l : listeners) {
						l.interfacesChanged(added, removed);
					}

					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.setDaemon(true);
		t.start();
	}

	public static class SelectedInterface {

		NetworkInterface nif;

		InetAddress inet;

		public SelectedInterface(NetworkInterface nif, InetAddress inet) {
			super();
			this.nif = nif;
			this.inet = inet;
		}

		public InetAddress getInet() {
			return inet;
		}

		public NetworkInterface getNif() {
			return nif;
		}

		@Override
		public String toString() {
			return nif + inet.toString();
		}
	}

	public static List<SelectedInterface> getLocalAddressIPv4() {
		return getLocalAddress(true);
	}

	public static List<SelectedInterface> getLocalAddress(boolean onlyipv4) {
		List<SelectedInterface> available = new ArrayList<>();
		Enumeration<NetworkInterface> en;
		try {
			en = NetworkInterface.getNetworkInterfaces();

			while (en.hasMoreElements()) {
				NetworkInterface nif = (NetworkInterface) en.nextElement();
				if (nif.getDisplayName().contains("Teredo"))
					continue;
				Enumeration<InetAddress> inet = nif.getInetAddresses();
				while (inet.hasMoreElements()) {
					InetAddress inetAddress = inet.nextElement();

					if (!inetAddress.isLoopbackAddress()
							&& !inetAddress.isLinkLocalAddress()
							&& (!onlyipv4 || !(inetAddress instanceof Inet6Address)))
						available.add(new SelectedInterface(nif, inetAddress));
				}
			}

		} catch (SocketException e1) {
			e1.printStackTrace();
		}
		if (available.size() == 0)
			try {
				available.add(new SelectedInterface(NetworkInterface
						.getByInetAddress(InetAddress.getLocalHost()),
						InetAddress.getLocalHost()));
			} catch (SocketException | UnknownHostException e) {
				e.printStackTrace();
			}

		return available;
		// if (available.size() >= 1) {
		// if (available.size() > 1) {
		// StringBuilder ifaces = new StringBuilder();
		// for (SelectedInterface selectedInterface : available) {
		// ifaces.append("," + selectedInterface.getNif().getName());
		// }
		// log.info("More than one available interface, using  "
		// + available.get(0).getNif().getName() + " from list :"
		// + ifaces.substring(1));
		// }else
		// log.info("Using interface "
		// + available.get(0).getNif().getName());
		// return available.get(0);
		// } else {
		// log.warn("Using localhost.");
		// }
		// try {
		// return new SelectedInterface(
		// NetworkInterface.getByInetAddress(InetAddress
		// .getLocalHost()), InetAddress.getLocalHost());
		// } catch (SocketException | UnknownHostException e) {
		// e.printStackTrace();
		// }
		// return null;
	}

	public static NetworkInterface getNif() {
		Enumeration<NetworkInterface> en;
		try {
			en = NetworkInterface.getNetworkInterfaces();
			while (en.hasMoreElements()) {
				return (NetworkInterface) en.nextElement();
			}
		} catch (SocketException e1) {
			e1.printStackTrace();
		}
		return null;
	}

	public static String getLocalAddressIPv6() throws IOException {
		// NetworkInterface nif = null;
		// int id = 0;
		// while (nif == null) {
		// try {
		// nif = NetworkInterface.getByName("eth" + id);
		// } catch (SocketException e) {
		// e.printStackTrace();
		// }
		// id++;
		// }
		// Enumeration<InetAddress> inet = nif.getInetAddresses();
		// while (inet.hasMoreElements()) {
		// InetAddress inetAddress = inet.nextElement();
		// if (!inetAddress.isLinkLocalAddress())
		// return inetAddress.getHostAddress();
		// }
		String host = Inet6Address.getLocalHost().getHostName();
		Logger log = Logger.getLogger(NetworkUtils.class);
		if (log.isDebugEnabled())
			log.debug("HostName : " + host);
		InetAddress[] addresses = Inet6Address.getAllByName(host);
		for (InetAddress inetAddress : addresses) {
			if (inetAddress instanceof Inet6Address) {
				return inetAddress.getHostAddress();
			}
		}
		return "";
	}

	public static void addNetworkChangeListener(NetworkChangeListener list) {
		listeners.add(list);
	}

	public static String getFirstHostAddress() {
		return getFirstHostAddress(false);
	}

	public static String getFirstHostAddress(boolean ipv4) {
		return getLocalAddress(ipv4).get(0).getInet().getHostAddress();
	}
}
