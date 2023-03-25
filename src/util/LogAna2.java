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
public class LogAna2 {
    private static String fileName = Config.logAna2ResultFile;

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
        LogAna2.log("I love you");
    }
}
