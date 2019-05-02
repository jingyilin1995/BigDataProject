package finalproject.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartiArrest {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PartiArrest");
		job.setJarByClass(PartiArrest.class);
		
		job.setMapperClass(PartiArrestMapper.class);
		job.setReducerClass(PartiArrestReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(PartiPartitioner.class);
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		
		public static class PartiArrestMapper extends Mapper<Object, Text, Text, NullWritable> {
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				context.write(value,NullWritable.get());
			}
		}
		public static class PartiPartitioner extends Partitioner<Text, NullWritable> {

			@Override
			public int getPartition(Text key, NullWritable value, int numPartitions) {
				String[] line = key.toString().split(";");
				String y = line[1].substring(0,4);
				int year = Integer.parseInt(y.toString());
				int partition = 0;
				if(numPartitions == 0) {
					partition = 0;
				}
				if(year<2013) {
					partition = 0;
				}
				else if(2013<=year && year<= 2015) {
					partition = 1 % numPartitions;
				}
				else {
					partition = 2 % numPartitions;
				}
				return partition;
			}
			
		}
		public static class PartiArrestReducer extends Reducer<Text, Text, Text, NullWritable> {
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
			}
		}
}
