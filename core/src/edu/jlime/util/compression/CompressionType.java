package edu.jlime.util.compression;

public enum CompressionType {
	LZ4(new LZ4Comp(), (byte) 0), SNAPPY(new SnappyComp(), (byte) 1), GZIP(
			new Gzip(), (byte) 2), BZIP(new Bzip(), (byte) 3), XZ(new XZ(),
			(byte) 4);
	private Compressor comp;
	private byte id;

	private CompressionType(Compressor comp, byte id) {
		this.comp = comp;
		this.id = id;
	}

	public Compressor getComp() {
		return comp;
	}

	public byte getId() {
		return id;
	}

	public static Compressor getByID(byte b) {
		for (CompressionType c : CompressionType.values()) {
			if (c.getId() == b)
				return c.getComp();
		}
		return null;
	}
}