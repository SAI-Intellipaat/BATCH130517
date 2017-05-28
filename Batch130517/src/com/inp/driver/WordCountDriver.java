package com.inp.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inp.mapper.WordCountMapper;
import com.inp.reducer.WordCountReducer;

public class WordCountDriver extends Configured implements Tool {

	public static void main(String[] args) 
	{
		//Total 3 steps
		
		//step; 1 : Validation
		
		if(args.length<2)
		{
			System.out.println("Java Usage:"+WordCountDriver.class.getName()+
					" [configuration] /path/to/hdfs/file /path/to/hdfs/destination/dir");
			return;
		}
		
		//step-2 : Load Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		
		try {
			
			//step 3 : ToolRunner.run
			int i=ToolRunner.run(conf, new WordCountDriver(), args);
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
		//Total 10 steps:
		
		//step-1 : Getting the configuration
		Configuration conf = super.getConf();
		
		//argument setting : You need to perform here
		
		//step-2: Create Job Instance
		
		Job wordCountJob = Job.getInstance(conf, WordCountDriver.class.getName());
		
		//step-3 : Setting the classpath on the mapper datanode
		wordCountJob.setJarByClass(WordCountDriver.class);
		
		//step-4
		//setting the input
		final String hdfsInput=args[0];
		//convert into path, because everything is URI
		final Path hdfsInputPath = new Path(hdfsInput);
		//Assing the Input Format
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		//set to the job 
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5 
		//setting output
		final String hdfsOutputDir=args[1];
		//convert into path, because everything is URI
		final Path hdfsOutputDirPath=new Path(hdfsOutputDir);
		//Assign the output format
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputDirPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		//Step-6 : Set the mapper 
		wordCountJob.setMapperClass(WordCountMapper.class);
		
		//step-7 : Set Mapper Ouput Key and Value Class
		//step-8 Set the reducer
		wordCountJob.setReducerClass(WordCountReducer.class);
		//step-9 : set the reducer output key and value classes
		//step-10: Trigger method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}


}
