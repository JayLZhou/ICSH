package util;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Date;

/**
 * @author fangyixiang
 * @date Jul 31, 2015
 * This class is used to log some information as required
 * The file name of the log file is defined as the current time
 */
public class LogPart {
    private static String fileName = Config.logPartResultFile;

    public static void log(String msg) {
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


    public static void main(String args[]) {
        LogPart.log("I love you");
    }
}
