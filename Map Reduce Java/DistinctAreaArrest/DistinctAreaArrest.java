package vertical.split;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctAreaArrest {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DistinctAreaArrest");
		job.setJarByClass(DistinctAreaArrest.class);
		job.setMapperClass(DistinctAreaArrestMapper.class);
		job.setCombinerClass(DistinctAreaArrestReducer.class);
		job.setReducerClass(DistinctAreaArrestReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class DistinctAreaArrestMapper extends Mapper<Object,Text,Text,NullWritable>{
		private Text area = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if(value.toString().contains("Report ID")) {
				return;
			}
			String[] list = value.toString().split(";");
			area.set(list[3]+"\t"+list[4]);
			context.write(area, NullWritable.get());
		}
	}
	public static class DistinctAreaArrestReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key,NullWritable.get());
		}
	}
}
