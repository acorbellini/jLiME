package edu.jlime.graphly.rec.salsa;

import java.util.Map;

public class SalsaResult {

	private Map<Long, Float> auth;
	private Map<Long, Float> hub;

	public SalsaResult(Map<Long, Float> auth, Map<Long, Float> hub) {
		this.auth = auth;
		this.hub = hub;
	}

	public static SalsaResult build(Map<Long, Float> map, Map<Long, Float> map2) {
		return new SalsaResult(map, map2);
	}

	public Map<Long, Float> getAuth() {
		return auth;
	}

	public Map<Long, Float> getHub() {
		return hub;
	}

}