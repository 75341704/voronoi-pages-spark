package uq;
 
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uq.spark.index.Page;
import uq.spark.index.PageIndex;

/**
 * A factory to format a Page as a file for  
 * Hadoop output 
 * 
 * @author uqdalves
 *
 * @param <K> PageIndex
 * @param <V> Page
 * 
 * http://johnnyprogrammer.blogspot.com.au/2012/01/custom-file-output-in-hadoop.html
 * https://developer.yahoo.com/hadoop/tutorial/module5.html
 */
public class PageOutputFormat extends FileOutputFormat<PageIndex, Page> {

	@Override
	public RecordWriter<PageIndex, Page> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
	     //get the current path
	     Path path = FileOutputFormat.getOutputPath(arg0);
	     //create the full path with the output directory plus our filename
	     Path fullPath = new Path(path, "pages.txt");

	     //create the file in the file system
	     FileSystem fs = path.getFileSystem(arg0.getConfiguration());
	     FSDataOutputStream fileOut = fs.create(fullPath, arg0);

	     //create our record writer with the new file
	     return new PageWriter(fileOut);
	}
	
}
