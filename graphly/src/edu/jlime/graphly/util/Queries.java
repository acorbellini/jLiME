package edu.jlime.graphly.util;

public class Queries {
	public static QueryContainer commonneighbours() {
		return new CommonNeightBoursQC();
	}

	public static QueryContainer hits() {
		return new HITSQC();

	}

	public static QueryContainer salsa() {
		return new SALSAQC();

	}

	public static QueryContainer wtf() {
		return new WhoToFollowQC();

	}

	public static QueryContainer ec() {
		return new ExploratoryCountQC();

	}

}
