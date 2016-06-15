package uq;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uq.spark.index.Page;
import uq.spark.index.PageIndex;

/**
 * A Writer to write pages to Hadoop OutputFormat.
 * 
 * Write a page as key/value pair in a XML format.
 *<PageIndex>Page</PageIndex>
 * 
 * @author uqdalves
 * 
 */
public class PageWriter extends RecordWriter<PageIndex, Page> {
	private DataOutputStream out;
    
    public PageWriter(DataOutputStream out) throws IOException {
	      this.out = out;
	      out.writeBytes("<pages>\n");
    }

	@Override
	public synchronized void close(TaskAttemptContext arg0) 
			throws IOException, InterruptedException {
		try {
			 out.writeBytes("</pages>\n");
		 } finally {
			 // close our file
			 out.close();
		 }
	}

	@Override
	public synchronized void write(PageIndex key, Page value) 
			throws IOException, InterruptedException {
	    // write key/value pairs
		writeKey(key, false);
		writeObject(value);
	    writeKey(value, true);
	}
	
    /**
     * Write the page index (key)
     */
    private void writeKey(Object key, boolean closing) throws IOException {
        out.writeBytes("<");
        if(closing) {
          out.writeBytes("/");
        }
        writeObject(key);
        out.writeBytes(">");
        if(closing) {
          out.writeBytes("\n");
        }
    }
    
	/**
	 * Write the key and values
	 */
    private void writeObject(Object obj) throws IOException {
    	if (obj instanceof PageIndex) {
        	PageIndex index = (PageIndex) obj;
        	index.write(out);
        	//out.writeChars(index.toString());
        } else {
        	Page page = (Page) obj;
//      	page.write(out);
        	//out.writeChars(page.toString());
        }
    }
}
