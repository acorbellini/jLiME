package edu.jlime.core.rpc;

import java.util.HashMap;
import java.util.Map.Entry;

public class RPCTemplate {

	private String templateString;

	private HashMap<String, String> replacements = new HashMap<>();

	public void putReplacement(String from, String to) {
		replacements.put(from, to);
	}

	public RPCTemplate(String string) {
		this.templateString = string;
	}

	public String build() {
		String ret = templateString;
		for (Entry<String, String> e : replacements.entrySet())
			ret = ret.replaceAll("\\$" + e.getKey(), e.getValue());
		return ret;
	}

}
