package edu.jlime.core.util;

import java.util.HashMap;
import java.util.Map;

public class DataUtil {

	public static Map<String, String> map(String... kv) {
		Map<String, String> ret = new HashMap<>();
		for (int i = 0; i < kv.length; i++) {
			String[] split = kv[i].split(":");
			ret.put(split[0], split[1]);
		}
		return ret;
	}

}
