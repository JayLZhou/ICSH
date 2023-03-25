package util;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class Log {
	public String fileName = null;
	
	public Log(String file) {
		fileName = file;
	}
	
	public void log(String msg) {
		try {
			BufferedWriter stdout = new BufferedWriter(new FileWriter(fileName, true));
			stdout.write(msg);
			stdout.newLine();			
			stdout.flush();
			stdout.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
