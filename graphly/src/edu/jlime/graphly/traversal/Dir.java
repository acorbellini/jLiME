package edu.jlime.graphly.traversal;

public enum Dir {

	IN((byte) 0),

	OUT((byte) 1),

	BOTH((byte) 2);

	byte id;

	private Dir(byte id) {
		this.id = id;
	}

	public Byte getID() {
		return this.id;
	}

	public static Object fromID(byte b) {
		for (Dir d : values()) {
			if (d.getID().equals(b))
				return d;
		}
		return null;
	}
}
