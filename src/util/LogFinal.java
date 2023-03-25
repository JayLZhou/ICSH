package util;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.Date;

/**
 * @author fangyixiang
 * @date Jul 31, 2015
 * This class is used to log some information as required
 * The file name of the log file is defined as the current time
 */
public class LogFinal {
	private static String fileName;
	
	public static void log(String msg, int type) {
		if (type == 2) {
			fileName = Config.logFinalResult2File;
		} else {
			fileName = Config.logFinalResult3File;
		}
		try {
			Date date = new Date();
			String time = date.toLocaleString();
			
			BufferedWriter stdout = new BufferedWriter(new FileWriter(fileName, true));
			stdout.write(time);
			stdout.write("\t");
			stdout.write(msg);
			stdout.newLine();
			
			stdout.flush();
			stdout.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String format(double value) {
		long longPart = (long)value;
		double restPart = value - longPart;
		DecimalFormat df = new DecimalFormat("#.00000");
		return longPart + "" + df.format(restPart);
	}
	
	public static void main(String args[]) {
		util.LogFinal.log("I love you", 2);
		DecimalFormat df = new DecimalFormat("#.000");
		System.out.println(df.format(-0.0000000125));
	}
}
