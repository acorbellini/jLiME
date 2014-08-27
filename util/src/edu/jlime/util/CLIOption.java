package edu.jlime.util;

public class CLIOption {

	private String name;

	private String shortName;

	private String desc;

	private int valNum;

	private String val;

	private String defaultValue;

	public CLIOption(String name, String shortName, String desc, int vals,
			String defaultValue) {
		super();
		this.setName(name);
		this.setShortName(shortName);
		this.setDesc(desc);
		this.setValNum(vals);
		this.defaultValue = defaultValue;
	}

	public boolean validateVal(String val) {
		return true;
	}

	public String getDefault() {
		return defaultValue;
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CLIOption))
			return false;
		return getName().equals(((CLIOption) obj).getName());
	}

	public void setValue(String val) {
		this.val = val;
	}

	public String getVal() {
		if (val != null)
			return val;
		return defaultValue;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public int getValNum() {
		return valNum;
	}

	public void setValNum(int valNum) {
		this.valNum = valNum;
	}
}