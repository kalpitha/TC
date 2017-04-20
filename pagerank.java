
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class pagerank
{
	
	public static class Matrix_Mapper1 extends Mapper<LongWritable,Text,Text,Text>
    {
	
		@Override
		protected void map(LongWritable key, Text value,Context context)
							throws IOException, InterruptedException
		{
			System.out.println("Inside Map !");
			String line = value.toString();
			String[] entry = line.split(",");
			String sKey = "";
			String mat = entry[0].trim();
			
			String row, col;
			
			Configuration conf = context.getConfiguration();
			String dimension = conf.get("dimension");
			
			System.out.println("Dimension from Mapper = " + dimension);
			
			int dim = Integer.parseInt(dimension);
			
			
			if(mat.matches("a"))
			{
				for (int i =0; i < dim ; i++) // hard coding matrix size 5 
				{
					row = entry[1].trim(); // rowid
					sKey = row+i;
					System.out.println(sKey + "-" + value.toString());
					context.write(new Text(sKey),value);
				}
			}
			
			if(mat.matches("b"))
			{
				for (int i =0; i < dim ; i++)
				{
					col = entry[2].trim(); // colid
					sKey = i+col;
					System.out.println(sKey + "-" + value.toString());
					context.write(new Text(sKey),value);
				}
			}
			
		}
	

    }
	
	
	
	
	public static class Matrix_Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
	
			@Override
			protected void reduce(Text key, Iterable<Text> values,Context context)
								throws IOException, InterruptedException 
			{
				
				Configuration conf = context.getConfiguration();
				String dimension = conf.get("dimension");
				
				int dim = Integer.parseInt(dimension);
				
				double[] row = new double[dim];
				double[] col = new double[dim];
				
				for(Text val : values)
				{
					String[] entries = val.toString().split(",");
					if(entries[0].matches("a"))
					{
						int index = Integer.parseInt(entries[2].trim());
						row[index] = Double.parseDouble(entries[3].trim());
					}
					if(entries[0].matches("b"))
					{
						int index = Integer.parseInt(entries[1].trim());
						col[index] = Double.parseDouble(entries[3].trim());
					}
				}
				
				double total = 0;
				for(int i = 0 ; i < 4; i++)
				{
					total += row[i]*col[i];
				}
				System.out.println(key.toString() + "-" + total );
                                String ke=key.toString();
                                String k[] = ke.split("");
                                Text t = new Text("b,"+k[0]+","+k[1]+",");
				context.write(t,new DoubleWritable(Math.round(total * 100.0) / 100.0));
			
			}
	
    }
	
	
	
	
	
	
	public static class Matrix_Mapper extends Mapper<LongWritable,Text,Text,Text>
    {
	
		@Override
		protected void map(LongWritable key, Text value,Context context)
							throws IOException, InterruptedException
		{
			System.out.println("Inside Map !");
			String line = value.toString();
			String[] entry = line.split(",");
			String sKey = "";
			String mat = entry[0].trim();
			
			String row, col;
			
			Configuration conf = context.getConfiguration();
			String dimension = conf.get("dimension");
			
			System.out.println("Dimension from Mapper = " + dimension);
			
			int dim = Integer.parseInt(dimension);
			
			
			if(mat.matches("a"))
			{
				for (int i =0; i < dim ; i++) // hard coding matrix size 5 
				{
					row = entry[1].trim(); // rowid
					sKey = row+i;
					System.out.println(sKey + "-" + value.toString());
					context.write(new Text(sKey),value);
				}
			}
			
			if(mat.matches("b"))
			{
				for (int i =0; i < dim ; i++)
				{
					col = entry[2].trim(); // colid
					sKey = i+col;
					System.out.println(sKey + "-" + value.toString());
					context.write(new Text(sKey),value);
				}
			}
			
		}
	

    }
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		int depth = 3;
		boolean IsEqual = false;
		while(IsEqual==false || depth<11){
			System.out.println("inside while");
			Configuration conf = new Configuration();
			conf.set("dimension", "4");
			Job job = Job.getInstance(conf);
			
			FileSystem fs = FileSystem.get(conf);
			job.setJobName("page rank");
			
			job.setJarByClass(pagerank.class);
			
			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
					
			job.setMapperClass(Matrix_Mapper.class);
			job.setMapperClass(Matrix_Mapper1.class);
			job.setReducerClass(Matrix_Reducer.class);
					
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path input1 = new Path("/usr/local/hadoop/input_test1/mat1.csv");
			Path input2 = new Path("/usr/local/hadoop/input_test2/mat"+(depth-1)+".csv");
			Path output = new Path("/usr/local/hadoop/input_test2/mat"+(depth)+".csv");
					
			if(!fs.exists(input1)) 
			{
				System.err.println("Input file1 doesn't exists");
				return;
			}
								
			if(!fs.exists(input2)) 
			{
				System.err.println("Input file2 doesn't exists");
				return;
			}
			if(fs.exists(output)) 
			{
				fs.delete(output, true);
				System.err.println("Output file deleted");
			}
			fs.close();
					
			MultipleInputs.addInputPath(job, input1, TextInputFormat.class,Matrix_Mapper.class);
			MultipleInputs.addInputPath(job, input2, TextInputFormat.class,Matrix_Mapper1.class);
			FileOutputFormat.setOutputPath(job, output);
			
			job.waitForCompletion(true);
			FileSystem fs3 =  null;
			FileSystem fs1 = null;
			BufferedReader br = null;
			BufferedReader br1 = null;
				try{
						Path pt=new Path("/usr/local/hadoop/input_test2/mat"+(depth-1)+".csv");
						Path pt1 = new Path("/usr/local/hadoop/input_test2/mat"+(depth)+".csv");
							fs = FileSystem.get(new Configuration());
							 fs1 = FileSystem.get(new Configuration());
							 br=new BufferedReader(new InputStreamReader(fs.open(pt)));
							 br1=new BufferedReader(new InputStreamReader(fs.open(pt)));
							String line;
							String line1;
							line=br.readLine();
							line1=br1.readLine();
							while (line != null && line1 != null){
                                                                String[] r = line.split(",");
                                                                String[] r1 = line1.split(",");
                                                                r[3] = r[3].trim();
                                                                r1[3] = r1[3].trim();
                                                                String l1 = r[0]+","+r[1]+","+r[2]+","+r[3];
                                                                String l2 = r1[0]+","+r1[1]+","+r1[2]+","+r1[3];
                                                                line.replaceAll(line,l1);
                                                                line1.replaceAll(line1,l2);
								if(l1.equals(l2)){
								 line=br.readLine();
								line1=br1.readLine();
								 IsEqual=true;
								}
								else{
									IsEqual=false;
									break;
								}
							}
				}
				catch(IOException e)
				{ 
				}
				finally{
					if(br!=null){
							try {
								br.close();
							} catch (IOException e) {
								
							 }
					}
					if(br1!=null){
							try {
								br1.close();
							} catch (IOException e) {
								
							}
					}
					
				
					
				}
					depth++;
			System.out.println("MR Job Completed !");
		}
		
	}

}
