package com.inp;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//java -cp Our.jars com.inp.HDFSService WordCount.txt /user/cloudera
public class HDFSService extends Configured implements Tool {
	
	public static void main(String[] args) 
	{
		//Total 3 steps
		
		//step; 1 : Validation
		
		if(args.length<2)
		{
			System.out.println("Java Usage:"+HDFSService.class.getName()+
					" [configuration] /path/to/edgenode/local/file /path/to/hdfs/destination/dir");
			return;
		}
		
		//step-2 : Load Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		try {
			
			//step 3 : ToolRunner.run
			int i=ToolRunner.run(conf, new HDFSService(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception 
	{
//		Configuration conf=new Configuration();//Completely Wrong
		
		
		Configuration conf=super.getConf();
		
		FileSystem hdfs = FileSystem.get(conf);
		
		//Input
		final String input=args[0];//wordcount.txt
		
		final String hdfsOutputDir=args[1];//user/cloudera
		
		//File Write = create metadata + add data
		
		//Creating Metadata = create empty file
		final Path fileNameWithDestinationLocation = new Path(hdfsOutputDir, input);//"/user/cloudera/WordCount.txt";
		
		FSDataOutputStream fos=hdfs.create(fileNameWithDestinationLocation);
		
		InputStream is = new FileInputStream(input);
		
		IOUtils.copyBytes(is, fos, conf,Boolean.TRUE);
		
		return 0;
	}

	

}
