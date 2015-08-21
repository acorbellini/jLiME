package edu.jlime.graphly.rec;

public class FriendLinkBeta implements BetaCalc {

	private long vcount;

	public FriendLinkBeta(long vcount) {
		this.vcount = vcount;
	}

	@Override
	public float calc(int depth) {
		float prod = 1f;
		for (int i = 2; i <= depth; i++) {
			prod *= vcount - i;
		}
		return (1 / (float) depth) * (1 / prod);
	}

	@Override
	public boolean mustSave(int i) {
		return (i >= 2);
	}

}
