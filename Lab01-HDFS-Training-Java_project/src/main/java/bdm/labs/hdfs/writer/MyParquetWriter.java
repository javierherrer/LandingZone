package bdm.labs.hdfs.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import adult.avro.Adult;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class MyParquetWriter implements MyWriter {
	
	private Configuration config;
	private FileSystem fs;
	
	
	AvroParquetWriter parquetWriter;
	
	public MyParquetWriter() throws IOException {
		this.config = new Configuration();
		parquetWriter = null;
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
		Path path = new Path("hdfs://10.4.41.51:27000"+ userPath + file);
		if (this.fs.exists(path)) {
			System.out.println("File "+file+" already exists!");
			System.exit(1);
		}
		parquetWriter = new AvroParquetWriter<GenericRecord>(path, Adult.SCHEMA$,
				CompressionCodecName.UNCOMPRESSED,
		          ParquetWriter.DEFAULT_BLOCK_SIZE,
		          ParquetWriter.DEFAULT_PAGE_SIZE,
		          true,config);
		/*try {
			parquetWriter = (AvroParquetWriter<Object>) AvroParquetWriter.builder(new Path(new URI("hdfs://10.4.41.154:27000/"+file))).build();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	
	public void put(Adult a) {
		try {
			this.parquetWriter.write(a);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reset() {
		
	}
	
	
	public int flush() throws IOException {
		return 1;
	}
	
	public void close() throws IOException {
		this.parquetWriter.close();
	}
	
}
