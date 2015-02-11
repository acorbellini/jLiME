package edu.jlime.graphly.traversal;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.recommendation.ForEach;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;

public class GraphlyTraversal implements Serializable, Transferible {

	Map<String, Object> vars = new HashMap<>();

	List<Step<?, ?>> steps = new ArrayList<>();

	int curr = 0;

	private Object currres;

	private transient Graphly g;

	public GraphlyTraversal(long[] ids, Graphly g) {
		this.currres = ids;
		this.g = g;
	}

	public Object submit(ClientNode c) throws Exception {
		return c.exec(new TraversalJob(this));
	}

	public <I, O> O next() throws Exception {
		Step<I, O> step = (Step<I, O>) steps.get(curr++);
		return (O) (this.currres = step.exec((I) currres));
	}

	public Object exec() throws Exception {
		for (int i = curr; i < steps.size(); i++) {
			next();
		}
		return this.currres;
	}

	public GraphlyTraversal out() {
		return to(Dir.OUT);
	}

	public GraphlyTraversal to(Dir dir) {
		addStep(new VertexStep(dir, this));
		return this;
	}

	private <I, O> void addStep(Step<I, O> vertexStep) {
		steps.add(vertexStep);
	}

	public Object get(String k) {
		return vars.get(k);
	}

	public GraphlyTraversal set(String k, Object v) {
		vars.put(k, v);
		return this;
	}

	public GraphlyTraversal in() {
		return to(Dir.IN);
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		JobDispatcher jd = (JobDispatcher) rpc.getTarget("JD");
		this.g = (Graphly) jd.getGlobal("graphly");
	}

	public Graphly getGraph() {
		return g;
	}

	public GraphlyTraversal filter(long[] first) {
		addStep(new FilterStep(first, this));
		return this;
	}

	public GraphlyTraversal count(Dir dir) {
		addStep(new CountStep(dir, this));
		return this;
	}

	public GraphlyTraversal countOut() {
		return count(Dir.OUT);
	}

	public <T extends CustomTraversal> T as(Class<T> c) throws Exception {
		Constructor<T> cons = c.getConstructor(GraphlyTraversal.class);
		return cons.newInstance(this);
	}

	public Object getCurrres() {
		return currres;
	}

	public GraphlyTraversal filterBy(String... k) {
		addStep(new VarFilterStep(k, this));
		return this;
	}

	public GraphlyTraversal save(String k) {
		addStep(new SaveStep(k, this));
		return this;
	}

	public GraphlyTraversal traverse(String[] filters, Dir... dirs)
			throws Exception {

		if (dirs.length == 0)
			return this;

		for (int i = 0; i < dirs.length; i++) {
			to(dirs[i]);
			filterBy(filters);
		}

		return this;
	}

	public GraphlyTraversal randomOut() {
		return toRandom(Dir.OUT);
	}

	private GraphlyTraversal toRandom(Dir dir) {
		addStep(new RandomStep(dir, this));
		return null;
	}

	public <T> GraphlyTraversal repeat(int steps, ForEach<T> forEach) {
		addStep(new RepeatStep<T>(steps, forEach, this));
		return this;
	}
}
