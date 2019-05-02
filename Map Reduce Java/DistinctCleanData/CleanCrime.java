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

public class CleanCrime {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DistinctAreaCrime");
		job.setJarByClass(DistinctAreaArrest.class);
		job.setMapperClass(CleanCrimeMapper.class);
		job.setCombinerClass(CleanCrimeReducer.class);
		job.setReducerClass(CleanCrimeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class CleanCrimeMapper extends Mapper<Object,Text,Text,NullWritable>{
		private Text area = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if(value.toString().contains("DR Number")) {
				return;
			}
			String[] list = value.toString().split(";");
			String y = list[1].substring(0,4);
			String m = list[1].substring(5,7);
			area.set(list[0]+";"+list[1]+";"+y+";"+m+";"+list[2]+";"+list[3]+";"+list[4]+";"+list[6]+";"+list[7]+";"+list[10]+";"+list[11]+";"+list[12]+";"+list[13]+";"+list[15]+";"+list[17]+";"+list[20]+";"+list[23]+";"+list[25]);
			context.write(area, NullWritable.get());
		}
	}
	public static class CleanCrimeReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key,NullWritable.get());
		}
	}
}
