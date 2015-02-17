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
import edu.jlime.graphly.recommendation.CustomStep;
import edu.jlime.graphly.recommendation.CustomStep.CustomFunction;
import edu.jlime.graphly.recommendation.MinEdgeFilter;
import edu.jlime.graphly.recommendation.Repeat;
import edu.jlime.graphly.recommendation.Update;
import edu.jlime.graphly.recommendation.VertexFilter;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.graphly.traversal.each.EachStep;
import edu.jlime.graphly.traversal.each.ForEach;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import gnu.trove.list.array.TLongArrayList;

public class GraphlyTraversal implements Serializable, Transferible {

	Map<String, Object> vars = new HashMap<>();

	List<Step> steps = new ArrayList<>();

	int curr = 0;

	private TraversalResult currres;

	private transient Graphly g;

	public GraphlyTraversal(long[] ids, Graphly g) {
		this.currres = new VertexResult(TLongArrayList.wrap(ids));
		this.g = g;
	}

	public TraversalResult submit(ClientNode c) throws Exception {
		return c.exec(new TraversalJob(this));
	}

	public TraversalResult next() throws Exception {
		Step step = steps.get(curr++);
		return this.currres = step.exec(currres);
	}

	public TraversalResult exec() throws Exception {
		for (int i = curr; i < steps.size(); i++) {
			next();
		}
		return this.currres;
	}

	public GraphlyTraversal out() {
		return to(Dir.OUT);
	}

	public GraphlyTraversal to(Dir dir, int max_edges) {
		addStep(new VertexStep(dir, max_edges, this));
		return this;
	}

	private void addStep(Step vertexStep) {
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

	public GraphlyTraversal countOut(String k) {
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
		addStep(new VarFilterStep(k, this, true));
		return this;
	}

	public GraphlyTraversal save(String k) {
		addStep(new SaveStep(k, this));
		return this;
	}

	public GraphlyTraversal traverse(int max_edges, Dir... dirs) {
		return this.traverse(new String[] {}, max_edges, dirs);
	}

	public GraphlyTraversal traverse(String[] filters, int max_edges,
			Dir... dirs) {
		if (dirs.length == 0)
			return this;

		for (int i = 0; i < dirs.length; i++) {
			to(dirs[i], max_edges);
			filterBy(filters);
		}
		return this;
	}

	public GraphlyTraversal randomOut() {
		return random(Dir.OUT, new long[] {});
	}

	public GraphlyTraversal random(Dir dir, long[] subset) {
		addStep(new RandomStep(dir, subset, this));
		return this;
	}

	public <T> GraphlyTraversal each(int steps, String key, ForEach<T> forEach) {
		addStep(new EachStep(key, steps, forEach, this));
		return this;
	}

	public GraphlyTraversal join(String from, String to, Join join) {
		addStep(new JoinStep(from, to, join, this));
		return this;
	}

	public GraphlyTraversal filterNeg(long[] before) {
		addStep(new FilterStep(before, this, true));
		return this;
	}

	public GraphlyTraversal update(Update update) {
		addStep(new UpdateStep(update, this));
		return this;
	}

	public GraphlyTraversal repeat(int steps, Repeat<long[]> rfunc) {
		addStep(new RepeatStep(steps, rfunc, this));
		return this;
	}

	public GraphlyTraversal traverse(String[] strings, Dir[] copyOfRange) {
		return traverse(strings, Integer.MAX_VALUE, copyOfRange);
	}

	public GraphlyTraversal to(Dir dir) {
		return to(dir, -1);
	}

	public GraphlyTraversal filter(VertexFilter customFilter) {
		addStep(new CustomFilterStep(customFilter, this));
		return this;
	}

	public GraphlyTraversal customStep(CustomFunction f) {
		addStep(new CustomStep(this, f));
		return this;
	}

}
