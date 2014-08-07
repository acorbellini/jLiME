package edu.jlime.core.cluster;

import java.util.UUID;

public class ServerAddressParser {

	private static final String EXECUTOR = "exec";

	private static final String NOT_EXECUTOR = "noexec";

	public static String generate(String[] tags, IP ip, boolean isExec) {

		StringBuilder tag = new StringBuilder();

		for (String t : tags) {
			tag.append("-" + t);
		}
		if (isExec) {
			tag.append("-" + EXECUTOR);
		} else {
			tag.append("-" + NOT_EXECUTOR);
		}
		return tag.substring(1) + "/" + ip.toString() + "/" + UUID.randomUUID();
	}

	private boolean exec;

	private String id;

	private IP ip;

	private String[] tags;

	public ServerAddressParser(String srv) throws Exception {
		// Tags/IP/UUID
		try {
			String[] list = srv.split("/");
			this.setTags(list[0].split("-"));
			this.setIp(IP.toIP(list[1]));
			for (String t : tags) {
				if (t.equals(EXECUTOR))
					this.setExec(true);
				else if (t.equals(NOT_EXECUTOR))
					this.setExec(false);
			}

			this.setId(srv);
		} catch (Exception e) {
			throw new Exception("Error parsing address " + srv);
		}
	}

	public String getId() {
		return id;
	}

	public IP getIp() {
		return ip;
	}

	public String[] getTags() {
		return tags;
	}

	public boolean isExec() {
		return exec;
	}

	public void setExec(boolean isExec) {
		this.exec = isExec;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setIp(IP ip) {
		this.ip = ip;
	}

	public void setTags(String[] tags) {
		this.tags = tags;
	}
}
