package skywriting.examples.grep;
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
		    
		    boolean countFirst = (args.length < 2) ? true : Boolean.parseBoolean(args[1]);
		    if (!countFirst) fis.read();
			
			while (true) {
				Text word = new Text();
				IntWritable value = new IntWritable();
				try {
					if (countFirst) value.readFields(fis);
					word.readFields(fis);
					if (!countFirst) value.readFields(fis);
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
