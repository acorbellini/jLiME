package edu.jlime.util;

import java.text.DecimalFormat;

public class StringUtils {

	public static String printTitle(String[] info) {
		String title = "";
		title += "\n";
		title += "\n";
		title += "       _ _    _  _    ______\n";
		title += "||    / / \\  / \\/ \\__//  __/   ?\n";
		title += "||    | | |  | || |\\/||  \\     ?\n";
		title += "|| /\\_| | |_/\\ || |  ||  /_    ?\n";
		title += "|| \\____|____|_/\\_/  \\\\____\\   ?\n";
		title += "\n";

		for (String i : info)
			title = title.replaceFirst("\\?", i);
		title = title.replace("?", "");
		return title;
	}

	public static String readableFileSize(long size) {
		if (size <= 0)
			return "0";
		final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(size
				/ Math.pow(1024, digitGroups))
				+ " " + units[digitGroups];
	}

	public static String readableTime(long s) {
		// GridGain devuelve hasta nanosegundos...
		// s = s/1000;
		long sinMS = (s / 1000);
		return String.format("%dh:%02dm:%02ds:%02dms", sinMS / 3600,
				(sinMS % 3600) / 60, (sinMS % 60), s % 1000);
	}

	public static String printTitle() {

		return printTitle(new String[] {});
	}
}
