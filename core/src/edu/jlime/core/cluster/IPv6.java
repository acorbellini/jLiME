package edu.jlime.core.cluster;

public class IPv6 extends IP {

	public IPv6(String ip) {
		super(8);
		String[] split = ip.split(":");
		for (int i = 0; i < split.length; i++) {
			if (split[i].isEmpty())
				dir[i] = 0;
			else
				dir[i] = Integer.parseInt(split[i], 16);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < dir.length; i++) {
			int val = dir[i];
			if (val == 0)
				builder.append(":");
			else
				builder.append(":" + Integer.toHexString(val));
		}
		return builder.substring(1);
	}

	@Override
	public int maxDirValue() {
		return 1024;
	}

	@Override
	public String getType() {
		return "6";
	}
}
