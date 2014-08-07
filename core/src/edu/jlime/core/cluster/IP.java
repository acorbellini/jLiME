package edu.jlime.core.cluster;

import java.io.Serializable;

public abstract class IP implements Comparable<IP>, Serializable {

	int[] dir;

	public IP(int size) {
		dir = new int[size];
	}

	@Override
	public int compareTo(IP ip) {
		if (dir.length > ip.dir.length)
			return 1;
		if (dir.length < ip.dir.length)
			return -1;

		boolean greater = false;
		boolean lesser = false;
		for (int i = 0; i < dir.length && !greater && !lesser; i++) {
			if (dir[i] > ip.dir[i])
				greater = true;
			else if (dir[i] < ip.dir[i])
				lesser = true;

		}
		if (greater)
			return 1;
		if (lesser)
			return -1;
		return 0;

	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IPv4))
			return false;
		return ((IPv4) obj).compareTo(this) == 0;
	}

	public static IP toIP(String hostAddress) {
		if (hostAddress.contains(":"))
			return new IPv6(hostAddress);
		return new IPv4(hostAddress);
	}

	public int[] getDir() {
		return dir;
	}

	public abstract int maxDirValue();

	public void setLast(int i) {
		dir[dir.length - 1] = i;
	}

	public abstract String getType();
}
