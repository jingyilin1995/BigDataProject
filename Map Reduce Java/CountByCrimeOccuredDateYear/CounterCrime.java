package finalproject.counter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterCrime {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CounterCrime");
		job.setJarByClass(CounterCrime.class);
		job.setMapperClass(CounterCrimeMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int code = job.waitForCompletion(true) ? 0 : 1;
		if(code == 0) {
			for(Counter counter : job.getCounters().getGroup(CounterCrimeMapper.YEAR_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
			}
		}
		FileSystem.get(conf).delete(new Path(args[1]),true);
		System.exit(code);
	}
	public static class CounterCrimeMapper extends Mapper<Object, Text, NullWritable, NullWritable>{
		public static final String YEAR_COUNTER_GROUP = "Year";
		public static final String UNKNOWN_COUNTER="Unknown";
		public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
		private String[] YEAR = new String[] {
				"2010","2011","2012","2013","2014","2015","2016","2017","2018","2019"
		};
		private HashSet<String> YEARSet = new HashSet<String>(Arrays.asList(YEAR));
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(";");
			String year = line[1].substring(0,4);	
			if (year != null && !year.isEmpty()) {
				if (YEARSet.contains(year)) {
					context.getCounter(YEAR_COUNTER_GROUP, year).increment(1);
				}else {
					context.getCounter(YEAR_COUNTER_GROUP,UNKNOWN_COUNTER).increment(1);
				}
			}else {
				context.getCounter(YEAR_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
			}
		}
	}
}
