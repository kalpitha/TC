package org.hadoop.matrixmultiplication;


import java.io.BufferedReader;
//import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HIA 5.4.5 - Product of Vector with Matrix. Both Matrix and
 * Vector are specified using sparse representation.
 * Uses map-side join using DistributedCache (to hold the Vector). 
 */
@SuppressWarnings("deprecation")
public class MatrixMultiplication extends Configured implements Tool {
  
  public static class Mapper1 extends 
      Mapper<LongWritable,Text,LongWritable,DoubleWritable> {
    
    private Map<Long,Double> vector = new HashMap<Long,Double>();
    
    @Override
    protected void setup(Context ctx) 
        throws IOException, InterruptedException {
    	FileSystem fs=FileSystem.get(ctx.getConfiguration());
    	FSDataInputStream fin=fs.open(new Path("big_data/assignment3/vector.txt"));
    	BufferedReader reader=new BufferedReader(new InputStreamReader(fin ));
        String line = null;
        try {
          while ((line = reader.readLine()) != null) {
            String[] cols = line.toString().trim().split("\t");
            vector.put(Long.valueOf(cols[0]), Double.valueOf(cols[1]));
          }
        } finally {
          reader.close();
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      String[] cols = value.toString().trim().split(",");
      Long row = Long.valueOf(cols[0]);
      Long col = Long.valueOf(cols[1]);
      Double entry = Double.valueOf(cols[2]);
      if (vector.containsKey(col)) {
        // multiply matrix and vector entry with matching col value
        entry = entry * vector.get(col);
        ctx.write(new LongWritable(row), new DoubleWritable(entry));
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable> {
    
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, 
        Context ctx) throws IOException, InterruptedException {
      double sum = 0.0D;
      // then sum up all entry values for the given row
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      ctx.write(key, new DoubleWritable(sum));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "matrix-multiply-vector");
    FileSystem fs = FileSystem.get(new Configuration());
    if(fs.exists(new Path("big_data/assignment3/ans"))){
    	fs.delete(new Path("big_data/assignment3/ans"),true);
    }
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(MatrixMultiplication.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    boolean succ = job.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job failed, exiting");
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
	  boolean flag=true;
	  
	  do{
		  ArrayList<Double> prev=new ArrayList<>();
		  ArrayList<Double> current=new ArrayList<>();
		  FileSystem fs=FileSystem.get(new Configuration());
		  if(fs.exists(new Path(args[1]))){
			  System.out.println("oh");
			  flag=false;
			  FSDataInputStream fin=fs.open(new Path("big_data/assignment3/vector.txt"));
		    	BufferedReader reader=new BufferedReader(new InputStreamReader(fin ));
		        String line = null;
		        try {
		          while ((line = reader.readLine()) != null) {
		            String[] cols = line.toString().trim().split("\t");
		            prev.add(Double.parseDouble(cols[1]));
		          }
		        } finally {
		          reader.close();
		        }
		        System.out.println(prev);
		        fin=fs.open(new Path("big_data/assignment3/ans/part-r-00000"));
		    	reader=new BufferedReader(new InputStreamReader(fin ));
		        line = null;
		        try {
		          while ((line = reader.readLine()) != null) {
		            String[] cols = line.toString().trim().split("\t");
		            current.add(Double.parseDouble(cols[1]));
		          }
		        } finally {
		          reader.close();
		        }
		        System.out.println(current);
		        Iterator<Double>itp=prev.iterator();
		        Iterator<Double>itc=current.iterator();
		        while(itp.hasNext()&&itc.hasNext()){
		        	Double x=itp.next();
		        	Double y=itc.next();
		        	Double z=x-y;
		        	if(Math.abs(z)>0.000001){
		        		System.out.println("oh"+x+y+z);
		        		flag=true;
		        	}
		        }
		  }
		  if(flag){
			  if(fs.exists(new Path(args[1]+"/part-r-00000"))){
				  fs.delete(new Path("big_data/assignment3/vector.txt"), true);
				  FileUtil.copy(fs, new Path("big_data/assignment3/ans/part-r-00000"), fs, new Path("big_data/assignment3/vector.txt"), true, true, new Configuration());
			  }
			  System.out.println("one");
			  ToolRunner.run(new Configuration(), new MatrixMultiplication(), args);
			      
		  }
  }while(flag);
	  
  }
}