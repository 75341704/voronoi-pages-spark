package uq.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;

/**
 * Service to read/write from/to the 
 * Tachyon file system (in memory storage).
 * 
 * @author uqdalves
 *
 */
public class TachyonFileService implements Callable<Boolean> {
	  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
	  private final TachyonURI mFilePath;
	  private final InStreamOptions mReadOptions;
	  private final OutStreamOptions mWriteOptions;
	  private final int mNumbers = 20;

	  public TachyonFileService(TachyonURI filePath, ReadType readType,
		      WriteType writeType) {
		    mFilePath = filePath;
		    mReadOptions =
		        new InStreamOptions.Builder(ClientContext.getConf()).setReadType(readType).build();
		    mWriteOptions =
		        new OutStreamOptions.Builder(ClientContext.getConf()).setWriteType(writeType).build();
		  }
	  
	public Boolean call() throws Exception {
	    TachyonFileSystem tachyonClient = 
	    		TachyonFileSystemFactory.get();
	    writeFile(tachyonClient);
	    return readFile(tachyonClient);
	}
	
	/**
	 * Write a file to Tachyon file system.
	 * 
	 * @return Return true if file was written successfully.
	 */
	private void writeFile(TachyonFileSystem tachyonFileSystem)
	    throws IOException, TachyonException {
	    ByteBuffer buf = 
	    		ByteBuffer.allocate(mNumbers * 4);
	    buf.order(ByteOrder.nativeOrder());
	    for (int k = 0; k < mNumbers; k ++) {
	      buf.putInt(k);
	    }
	    LOG.debug("Writing data...");
	    long startTimeMs = CommonUtils.getCurrentMs();
	    FileOutStream os = tachyonFileSystem.getOutStream(mFilePath, mWriteOptions);
	    os.write(buf.array());
	    os.close();
	    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
	}
	
	/**
	 * Read a file from Tachyon file system.
	 * 
	 * @return Return true if file was read successfully.
	 */
	private boolean readFile(TachyonFileSystem tachyonFileSystem)
		      throws IOException, TachyonException {
	    boolean pass = true;
	    LOG.debug("Reading data...");
	    TachyonFile file = 
	    		tachyonFileSystem.open(mFilePath);
	    final long startTimeMs = CommonUtils.getCurrentMs();
	    FileInStream is = tachyonFileSystem.getInStream(file, mReadOptions);
	    ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
	    is.read(buf.array());
	    buf.order(ByteOrder.nativeOrder());
	    for (int k = 0; k < mNumbers; k ++) {
	      pass = pass && (buf.getInt() == k);
	    }
	    is.close();
	
	    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "readFile file " + mFilePath));
	    return pass;
	 }
}
