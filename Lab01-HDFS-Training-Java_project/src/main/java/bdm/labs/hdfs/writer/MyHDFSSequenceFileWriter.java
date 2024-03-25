package bdm.labs.hdfs.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import adult.avro.Adult;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import org.apache.hadoop.io.Text;

public class MyHDFSSequenceFileWriter implements MyWriter {
	
	private Configuration config;
	private FileSystem fs;
	private SequenceFile.Writer writer;
	private FSDataOutputStream output;
	
	private StringBuilder keyBuffer;
	private StringBuilder valueBuffer;
	
	public MyHDFSSequenceFileWriter() throws IOException {
		this.config = new Configuration();
		this.writer = null;
		this.reset();
	}

	public void open(String file) throws IOException {
		this.config = new Configuration();
		config.set("fs.hdfs.impl", 
		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		config.set("fs.file.impl",
		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
		try {
			this.fs = FileSystem.get(new URI("hdfs://10.4.41.51:27000"), config);
		}
		catch (URISyntaxException e) {
			e.printStackTrace();
		}
		Path path = new Path(userPath + file);
		if (this.fs.exists(path)) {
			System.out.println("File "+file+" already exists!");
			System.exit(1);
		}
		SequenceFile.Writer.Option[] options = new SequenceFile.Writer.Option[]
		{
			    SequenceFile.Writer.stream(this.fs.create(path)),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class),
				SequenceFile.Writer.compression(CompressionType.NONE)
//				SequenceFile.Writer.compression(CompressionType.RECORD)
//				SequenceFile.Writer.compression(CompressionType.BLOCK)
		};
		this.writer = SequenceFile.createWriter(this.config, options);	
		
	}
	
	public void put(Adult a) {
		this.keyBuffer.append(RandomStringUtils.randomAlphanumeric(8));
		this.valueBuffer.append(a.getAge()+","+a.getWorkclass()+","+a.getFnlwgt()+","+a.getEducation()+","+
				a.getEducationNum()+","+a.getMaritalStatus()+","+a.getRelationship()+","+a.getRace()+","+
				a.getSex()+","+a.getCapitalGain()+","+a.getCapitalLoss()+","+a.getHoursPerWeek()+","+a.getNativeCountry());
	}
	
	public void reset() {
		this.keyBuffer = new StringBuilder();
		this.valueBuffer = new StringBuilder();
	}
	
	public int flush() throws IOException {
		String key = this.keyBuffer.toString();
		String value = this.valueBuffer.toString();
		this.writer.append(new Text(key.toString()), new Text(value.toString()));		
		this.reset();
		return value.length();
	}
	
	public void close() throws IOException {
		this.writer.close();
		this.fs.close();
	}
	
}
