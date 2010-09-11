import java.io.EOFException;
import java.io.FileInputStream;
import java.io.*;


public class PrintHelper {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
		    DataInputStream fis = new DataInputStream(new BufferedInputStream(new FileInputStream (args[0])));
			
			while (true) {
				Text word = new Text();
				IntWritable value = new IntWritable();
				try {
					word.readFields(fis);
					value.readFields(fis);
				} catch (EOFException e) {
					break;
				}

				System.out.println(word + " = " + value);
			}
		} catch (Exception e) {
			System.out.println("Oooooops... : ");
			e.printStackTrace();
		}


	}

}
