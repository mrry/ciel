package skywriting.examples.smithwaterman;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class PartitionInputString implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		
		try {
			/**
			 * args: None.
			 */
			
			/**
			 * inputs: full input string. Whole thing will be read into memory.
			 */
			ByteArrayOutputStream inputStringBuffer = new ByteArrayOutputStream();
			int c;
			while ((c = fis[0].read()) != -1) {
				inputStringBuffer.write(c);
			}
			byte[] inputString = inputStringBuffer.toByteArray();
			
			/**
			 * outputs: n output blocks. 
			 */
			int numBlocks = fos.length;
			int blockLength = (inputString.length / numBlocks) + ((inputString.length % numBlocks == 0) ? 0 : 1);
			
			int currentPos = 0;
			for (int i = 0; i < numBlocks; ++i) {
				for (int j = 0; j < blockLength && currentPos < inputString.length; ++j, ++currentPos) {
					fos[i].write(inputString[currentPos]);
				}
				fos[i].flush();
				fos[i].close();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public static void main(String[] args) {
		try { 
			int numBlocks = Integer.parseInt(args[0]);
			ByteArrayOutputStream[] outputs = new ByteArrayOutputStream[numBlocks];
			for (int i = 0; i < numBlocks; ++i) {
				outputs[i] = new ByteArrayOutputStream();
			}
			
			FileInputStream input = new FileInputStream(args[1]);
			
			new PartitionInputString().invoke(
					new InputStream[] { input },
					outputs,
					new String[] {});
			
			for (int i = 0; i < numBlocks; ++i) {
				System.out.printf("Block %d: %s\n", i, outputs[i].toString());
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
