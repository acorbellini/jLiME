package edu.jlime.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class CLI {

	HashMap<String, CLIOption> optHash = new HashMap<>();

	public void flag(String name, String shortName, String desc) {
		optHash.put(name, new CLIOption(name, shortName, desc, 0, null));
	}

	public void param(String name, String shortName, String desc, String defaultVal) {
		optHash.put(name, new CLIOption(name, shortName, desc, 1, defaultVal));
	}

	public void parse(String[] args) throws ParseException {
		ArrayIterator<String> it = new ArrayIterator<>(args);
		while (it.hasNext()) {
			String arg = (String) it.next();
			String slash = arg.substring(0, 1);
			if (!slash.equals("-"))
				throw new ParseException("Could not parse argument.", 0);
			String param = arg.substring(1);
			fillVals(param, it);
		}
	}

	private void fillVals(String arg, ArrayIterator<String> it) throws ParseException {
		for (String k : optHash.keySet()) {
			CLIOption cli = optHash.get(k);
			if (arg.equals(cli.getName())) {
				if (cli.getValNum() == 1) {
					try {
						String val = it.peek();
						if (!cli.validateVal(val))
							throw new ParseException("Error validating arg " + arg + ".", 0);
						cli.setValue(val);
						if (val.startsWith("-"))
							throw new ParseException("Missing value for argument " + arg + ".", 0);
						else
							it.next();
					} catch (Exception e) {
						throw new ParseException("Missing value for argument " + arg + ".", 0);
					}

				}
				return;
			} else if (arg.startsWith(cli.getShortName())) {
				if (cli.getValNum() == 1) {
					String val = arg.substring(cli.getShortName().length(), arg.length());
					if (val.isEmpty())
						val = it.next();
					if (!cli.validateVal(val))
						throw new ParseException("Error validating arg " + arg + ".", 0);
					cli.setValue(val);
					return;
				}
			}
		}
		throw new ParseException("Could not parse argument " + arg + ".", 0);
	}

	public String get(String name) {
		return optHash.get(name).getVal();
	}

	public boolean contains(String name) {
		return optHash.containsKey(name);
	}

	public int getInt(String string) {
		return new Integer(optHash.get(string).getVal());
	}

	public void param(String name, String shortName, String desc, int i) {
		param(name, shortName, desc, i + "");
	}

	public void param(String name, String shortName, String desc) {
		param(name, shortName, desc, null);
	}

	public String getHelp(String title, String author, int size) {
		StringBuilder builder = new StringBuilder();
		builder.append(title + "\n\n");
		ArrayList<CLIOption> opts = new ArrayList<>(optHash.values());
		Collections.sort(opts, new Comparator<CLIOption>() {

			@Override
			public int compare(CLIOption o1, CLIOption o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		int maxNameChars = 0;
		int maxShortChars = 0;
		for (CLIOption opt : opts) {
			if (opt.getName().length() + 1 > maxNameChars)
				maxNameChars = opt.getName().length() + 1;
			if (opt.getShortName().length() + 1 > maxShortChars)
				maxShortChars = opt.getShortName().length() + 1;
		}
		for (CLIOption opt : opts) {
			int dif = maxNameChars - opt.getName().length();
			StringBuilder name = new StringBuilder(opt.getName());
			for (int i = 0; i < dif; i++) {
				name.append(" ");
			}
			builder.append("-" + name + "  ");
			int difShort = maxShortChars - opt.getShortName().length();
			StringBuilder shortName = new StringBuilder(opt.getShortName());
			for (int i = 0; i < difShort; i++) {
				shortName.append(" ");
			}
			builder.append("-" + shortName + "  ");

			StringBuilder desc = new StringBuilder();
			int row = 0;
			int cont = 0;
			int descSize = opt.getDesc().length();
			int leftSize = maxNameChars + maxShortChars + 6;
			int remaining = (size - leftSize);
			do {
				if (row > 0)
					for (int i = 0; i < leftSize; i++) {
						desc.append(" ");
					}
				int to = Math.min(opt.getDesc().length(), remaining + cont);
				desc.append(opt.getDesc().substring(cont, to) + "\n");
				cont = to;
				descSize -= remaining;
				row++;
			} while (descSize > remaining);
			if (descSize > 0) {
				for (int i = 0; i < leftSize; i++)
					desc.append(" ");

				desc.append(opt.getDesc().substring(cont, opt.getDesc().length()) + "\n");
			}
			builder.append(desc);
		}

		builder.append("\n");
		builder.append("Author: " + author + "\n");

		return builder.toString();
	}

	public void mult(String name, String shortName, String desc, String def, String[] posible) {
		optHash.put(name, new CLIMultiVal(name, shortName, desc, def, posible));
	}
}
