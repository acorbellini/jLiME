package edu.jlime.util;

import java.util.HashSet;

public class CLIMultiVal extends CLIOption {

	private HashSet<String> posible = new HashSet<>();

	public CLIMultiVal(String name, String shortName, String desc, String defaultValue, String[] posible) {
		super(name, shortName, desc, 1, defaultValue);
		for (String string : posible) {
			this.posible.add(string);
		}
	}

	@Override
	public boolean validateVal(String val) {
		return posible.contains(val);
	}

	@Override
	public String getDesc() {
		return super.getDesc() + " - Available Options: " + posible;
	}
}