package edu.jlime.rpc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;

public class RPCServerClassCreator {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException {
		String iface = args[0];

		Class serverInterface = Class.forName(iface);

		createBroadcastIface(serverInterface);

		createServer(serverInterface);

		createBroadcastServer(serverInterface);

		createFactory(serverInterface);

	}

	private static void createFactory(Class<?> serverInterface)
			throws IOException {
		String name = serverInterface.getSimpleName() + "Factory";

		String bcast = serverInterface.getSimpleName() + "Broadcast";

		String iface = serverInterface.getSimpleName() + "Server";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));
		writer.write("import edu.jlime.core.cluster.Peer;\n");
		writer.write("import edu.jlime.core.rpc.RPCDispatcher;\n");

		writer.write("import java.util.List;\n");
		writer.write("public class " + name + " {\n\n");

		writer.write("  private RPCDispatcher rpc;\n\n");
		writer.write("  private String target;\n\n");

		writer.write("  public " + name
				+ "(RPCDispatcher rpc, String target){\n");
		writer.write("     this.rpc = rpc;\n");
		writer.write("     this.target = target;\n");
		writer.write("  }\n");

		writer.write("  public " + bcast
				+ " getBroadcast(List<Peer> to, String cliID){\n");
		writer.write("    return new " + bcast
				+ "Impl(rpc, to, cliID, target);\n");
		writer.write("  }\n");

		writer.write("  public " + serverInterface.getSimpleName()
				+ " get(Peer to, String cliID){\n");
		writer.write("    return new " + iface
				+ "Impl(rpc, to, cliID, target);\n");
		writer.write("  }\n");
		writer.write("}");
		writer.close();
	}

	private static void createBroadcastServer(Class<?> serverInterface)
			throws IOException {
		String name = serverInterface.getSimpleName() + "BroadcastImpl";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));
		writer.write("import edu.jlime.core.cluster.BroadcastException;");
		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " implements "
				+ serverInterface.getSimpleName() + "Broadcast {\n\n");

		writer.write(getBroadcastFields());

		writer.write(getBroadcastConstructor(name));

		for (Method method : serverInterface.getMethods()) {
			writer.write(getBroadcastMethodSignature(method.getName(), method));
			writer.write(getBroadcastBody(method.getName(), method));
		}
		writer.write("}");
		writer.close();
	}

	private static String getBroadcastBody(String name, Method method) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");
		StringBuilder args = new StringBuilder();
		for (Class<?> arg : method.getParameterTypes()) {
			args.append("," + arg.getSimpleName().toLowerCase());
		}
		boolean sync = !method.getReturnType().getSimpleName().equals("void");
		String retType = method.getReturnType().getSimpleName();
		String singleCall = sync ? "multiCallSync" : "multiCallAsync";

		if (sync)
			builder.append("    return Map<Peer," + retType + "> ");
		else
			builder.append("    ");
		builder.append("disp." + singleCall + "( dest, cliID, targetID, \""
				+ method.getName() + "\",new Object[] { " + args.substring(1)
				+ " });\n");
		builder.append("  }\n\n");
		return builder.toString();
	}

	private static void createServer(Class<?> serverInterface)
			throws IOException {
		String name = serverInterface.getSimpleName() + "ServerImpl";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));

		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " implements "
				+ serverInterface.getSimpleName() + " {\n\n");

		writer.write(getFields());

		writer.write(getConstructor(name));

		for (Method method : serverInterface.getMethods()) {
			writer.write(getMethodSignature(method.getName(), method));
			writer.write(getBody(method.getName(), method));
		}
		writer.write("}");
		writer.close();
	}

	private static void createBroadcastIface(Class<?> ifaceClass)
			throws IOException {
		String name = ifaceClass.getSimpleName() + "Broadcast";
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));
		writer.write(getPackage(ifaceClass));
		writer.write("import edu.jlime.core.cluster.BroadcastException;");
		writer.write(getImports(ifaceClass));
		writer.write("public interface " + name + " { \n\n");

		for (Method m : ifaceClass.getMethods())
			writer.write(getBroadcastMethodSignature(m.getName(), m) + "; \n\n");

		writer.write("}");
		writer.close();
	}

	private static String getPackage(Class<?> ifaceClass) {
		return ifaceClass.getPackage() + ";\n\n";
	}

	private static String getFields() {
		StringBuilder field = new StringBuilder();
		field.append("  RPCDispatcher disp;\n");
		field.append("  Peer local;\n");
		field.append("  Peer dest;\n");
		field.append("  String cliID;\n");
		field.append("  String targetID;\n\n");
		return field.toString();
	}

	private static String getBroadcastFields() {
		StringBuilder field = new StringBuilder();
		field.append("  RPCDispatcher disp;\n");
		field.append("  Peer local;\n");
		field.append("  List<Peer> dest = new ArrayList<Peer>();\n");
		field.append("  String cliID;\n");
		field.append("  String targetID;\n\n");
		return field.toString();
	}

	private static String getConstructor(String name) {
		StringBuilder constructor = new StringBuilder();

		constructor
				.append("  public "
						+ name
						+ "(RPCDispatcher disp, Peer dest, String cliID, String targetID) {\n");
		constructor.append("    this.disp = disp;\n");
		constructor.append("    this.dest = dest;\n");
		constructor.append("    this.cliID = cliID;\n");
		constructor.append("    this.targetID = targetID;\n");
		constructor.append("  }\n\n");
		return constructor.toString();
	}

	private static String getBroadcastConstructor(String name) {
		StringBuilder constructor = new StringBuilder();
		constructor
				.append("  public "
						+ name
						+ "(RPCDispatcher disp, List<Peer> dest, String cliID, String targetID) {\n");
		constructor.append("    this.disp = disp;\n");
		constructor.append("    this.dest.addAll(dest);\n");
		constructor.append("    this.cliID = cliID;\n");
		constructor.append("    this.targetID = targetID;\n");
		constructor.append("  }\n\n");
		return constructor.toString();
	}

	private static String getMethodSignature(String name, Method method)
			throws IOException {
		StringBuilder ret = new StringBuilder();
		ret.append("  public " + method.getReturnType().getSimpleName() + " "
				+ name + "(");
		ret.append(getParameters(method));
		ret.append(") ");
		ret.append(getExceptions(method));
		return ret.toString();
	}

	private static String getBroadcastMethodSignature(String name, Method method)
			throws IOException {
		StringBuilder ret = new StringBuilder();
		String returnType = method.getReturnType().getSimpleName()
				.equals("void") ? "void" : "Map<Peer,"
				+ method.getReturnType().getSimpleName() + "> ";
		ret.append("  public " + returnType + " " + name + "(");
		ret.append(getParameters(method));
		ret.append(") throws Exception");
		return ret.toString();
	}

	private static String getExceptions(Method method) {
		StringBuilder ret = new StringBuilder();
		Class<?>[] types = method.getExceptionTypes();
		if (types.length != 0) {
			ret.append(" throws ");
			StringBuilder builder = new StringBuilder();
			for (Class<?> class1 : types) {
				builder.append("," + class1.getSimpleName());
			}
			ret.append(builder.substring(1));
		}
		return ret.toString();
	}

	private static String getParameters(Method method) {
		StringBuilder ret = new StringBuilder();
		Class<?>[] args = method.getParameterTypes();
		if (args.length != 0) {
			StringBuilder argsBuilder = new StringBuilder();
			for (Class<?> c : args) {
				argsBuilder.append(", " + c.getSimpleName() + " "
						+ c.getSimpleName().toLowerCase());
			}
			ret.append(argsBuilder.substring(2));
		}
		return ret.toString();
	}

	private static String getImports(Class ifaceClass) {
		StringBuilder imports = new StringBuilder();
		imports.append("import " + ifaceClass.getName() + ";\n");
		imports.append("import edu.jlime.core.rpc.RPCDispatcher;\n");
		imports.append("import edu.jlime.cluster.Peer;\n");
		imports.append("import java.util.List;\n");
		imports.append("import java.util.ArrayList;\n");

		for (Method m : ifaceClass.getMethods()) {
			for (Class<?> arg : m.getParameterTypes()) {
				if (!arg.isPrimitive()) {
					imports.append("import " + arg.getCanonicalName() + ";\n");
				}
				Class<?> ret = m.getReturnType();
				if (!ret.isPrimitive())
					imports.append("import " + ret.getCanonicalName() + ";\n");
			}
			for (Class<?> ex : m.getExceptionTypes()) {
				imports.append("import " + ex.getCanonicalName() + ";\n");
			}
		}
		return imports.toString() + "\n";
	}

	private static String getBody(String rpcmethod, Method method) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");
		StringBuilder args = new StringBuilder();
		for (Class<?> arg : method.getParameterTypes()) {
			args.append("," + arg.getSimpleName().toLowerCase());
		}
		boolean sync = !method.getReturnType().getSimpleName().equals("void");
		String retType = method.getReturnType().getSimpleName();
		String singleCall = sync ? "callSync" : "callAsync";
		if (sync)
			builder.append("    return (" + retType + ") ");
		else
			builder.append("    ");
		builder.append("disp." + singleCall + "(dest, cliID, targetID, \""
				+ method.getName() + "\",new Object[] { " + args.substring(1)
				+ " });\n");
		builder.append("  }\n\n");
		return builder.toString();
	}
}
