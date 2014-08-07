package edu.jlime.core.cluster;

public class IPv4 extends IP {

	private static final long serialVersionUID = -6349040907373056075L;

	public IPv4(String stringRep) {
		super(4);
		String[] split = stringRep.replaceAll("/", "").split("\\.");
		dir[0] = Integer.parseInt(split[0]);
		dir[1] = Integer.parseInt(split[1]);
		dir[2] = Integer.parseInt(split[2]);
		dir[3] = Integer.parseInt(split[3]);
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public String toString() {
		return dir[0] + "." + dir[1] + "." + dir[2] + "." + dir[3];
	}

	public static IPv4 toIP(String hostAddress) {
		return new IPv4(hostAddress);
	}

	@Override
	public int maxDirValue() {
		return 256;
	}

	@Override
	public String getType() {
		return "4";
	}

}
