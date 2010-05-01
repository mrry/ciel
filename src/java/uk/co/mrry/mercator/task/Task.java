package uk.co.mrry.mercator.task;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * A task takes zero or more named data items, performs some computation on them, and yields one or more concrete data items
 * as output.
 * 
 * To execute a task, the named input data must be available locally.
 * 
 * @author dgm36
 *
 */
public interface Task {

    public void invoke(FileInputStream[] fis, FileOutputStream[] fos, String[] args);
	
}
