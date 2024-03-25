package bdm.labs.hdfs.reader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class MyHDFSSequenceFileReader implements MyReader {

	private Configuration config;
	private FileSystem fs;
	private SequenceFile.Reader reader;
	
	public MyHDFSSequenceFileReader() {
		try {
			this.config = new Configuration();
			config.set("fs.hdfs.impl", 
			        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
			    );
			config.set("fs.file.impl",
			        org.apache.hadoop.fs.LocalFileSystem.class.getName()
			    );
            config.addResource(new Path("/home/bdm/BDM_Software/hadoop/etc/hadoop/core-site.xml"));
            this.fs = FileSystem.get(new URI("hdfs://HOST:27000"), config);
		} catch (IOException e) {
			e.printStackTrace();
		}
		 catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
	}
	
	public void open(String file) throws IOException {
		Path path = new Path(userPath + file);
		if (!this.fs.exists(path)) {
			System.out.println("File "+file+" does not exist!");
			System.exit(1);
		}
		SequenceFile.Reader.Option[] options = new SequenceFile.Reader.Option[]
		{				
				SequenceFile.Reader.stream(this.fs.open(path))
		};
		this.reader = new SequenceFile.Reader(this.config, options);
	}
	
	public String next() throws IOException {
		Text key = new Text();
		Text value = new Text();
		if (this.reader.next(key, value)) {
			return key.toString()+'\t'+value.toString();
		}
		return null;
	}
	
	public void close() throws IOException {
		this.reader.close();
		this.fs.close();
	}
	
}
