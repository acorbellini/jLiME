package edu.jlime.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class CommandLineUtils {

	private static boolean CYGWIN = System.getProperty("os.name").startsWith(
			"Windows") ? true : false;

	public static String execCommand(String cmd) throws Exception {
		ProcessBuilder procbuilder = null;
		if (CYGWIN) {
			File dir = new File("C:/cygwin/bin/bash.exe");
			File dir64 = new File("C:/cygwin64/bin/bash.exe");

			cmd = cmd.replaceAll("\"", "\\\\\"");
			cmd = "\"" + cmd + "\"";

			if (dir.exists())
				procbuilder = new ProcessBuilder("C:/cygwin/bin/bash.exe",
						"--login", cmd);
			else if (dir64.exists())
				procbuilder = new ProcessBuilder("C:/cygwin64/bin/bash.exe",
						"--login", cmd);
			else
				throw new Exception("Cygwin not found");

		} else
			procbuilder = new ProcessBuilder("/bin/sh", "-c", cmd);
		Process proc = procbuilder.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				proc.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String line = null;
		while ((line = br.readLine()) != null) {
			builder.append(line);
			builder.append(System.getProperty("line.separator"));
		}
		return builder.toString();

	}
}
