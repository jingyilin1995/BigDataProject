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


public class DistinctCharge {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DistinctCharge");
		job.setJarByClass(DistinctCharge.class);
		job.setMapperClass(DistinctChargeMapper.class);
		job.setCombinerClass(DistinctChargeReducer.class);
		job.setReducerClass(DistinctChargeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class DistinctChargeMapper extends Mapper<Object,Text,Text,NullWritable>{
		private Text area = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if(value.toString().contains("Report ID")) {
				return;
			}
			String[] list = value.toString().split(";");
			area.set(list[12]+"\t"+list[13]);
			context.write(area, NullWritable.get());
		}
	}
	public static class DistinctChargeReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key,NullWritable.get());
		}
	}
}
