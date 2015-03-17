package org.CMPT732A4;


import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.RecordReader;



public class HiveRecordReader implements RecordReader<LongWritable, Text> {
	
	private static final Log LOG = LogFactory.getLog(HiveRecordReader.class.getName());
	private static final int NUMBER_OF_FIELDS = 4;
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader lineReader;
	int maxLineLength;
	
	public HiveRecordReader(FileSplit inputSplit, Configuration job) throws IOException {
		maxLineLength = job.getInt("mapred.escapedlinereader.maxlength", Integer.MAX_VALUE);
		start = inputSplit.getStart();
		end = start + inputSplit.getLength();
		final Path file = inputSplit.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		
		// Open file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);
		boolean skipFirstLine = false;
		if (codec != null) {
			lineReader = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			lineReader = new LineReader(fileIn, job);
		}
		
		if (skipFirstLine) {
			start += lineReader.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	

	
	public HiveRecordReader(InputStream in, long offset, long endOffset, Configuration job)
		throws IOException {
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		this.lineReader = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset; 
	}
	
	public LongWritable createKey() {
		return new LongWritable();
	}
	
	public Text createValue() {
		return new Text();
	}
	
	/**
	 * Reads the next record in the split.  All instances of \\t, \\n and \\r\n are replaced by a space.
	 * @param key key of the record which will map to the byte offset of the record's line
	 * @param value the record in text format
	 * @return true if a record existed, false otherwise
	 * @throws IOException
	 */
	public synchronized boolean next(LongWritable key, Text value) throws IOException {
		// Stay within the split
		String str = new String("");
		Text temp = new Text();
		int count = 0;
		while (pos < end) {
			key.set(pos);
			int newSize = lineReader.readLine(temp, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			
			if (newSize == 0)
				return false;
			
			pos += newSize;
			
			if (temp.toString()!=null && temp.toString().equals("--") && count == NUMBER_OF_FIELDS ){
				value.set(str);
				return true;
			}
			else if(temp.toString()!=null ){
				str += temp.toString().split("=")[1] + "\t" ;
				count++;
				
			}
		}
		return false;
	}
	
	
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
	}
	
	public synchronized long getPos() throws IOException {
		return pos;
	}
	
	public synchronized void close() throws IOException {
		if (lineReader != null)
			lineReader.close();
	}
	
}