package edu.jlime.webmonitor;

import java.io.Serializable;

public class PeerData implements Serializable {

	private static final long serialVersionUID = 190426694695830974L;

	String ip;

	String info;

	public PeerData(String ip, String info) {
		super();
		this.ip = ip;
		this.info = info;
	}

	public String getInfo() {
		return info;
	}

	public String getIp() {
		return ip;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
