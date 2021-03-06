package tw.com.ruten.ts.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;


/*
 * simple MR start project 
 * 
 * @author : realmeat@staff.ruten.com.tw
 */

public class SimpleJob extends Configured implements Tool{

	public static Logger LOG = Logger.getLogger(SimpleJob.class);
	public Configuration conf;
	
	public static class TokenizerMapper  extends Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			LOG.info("line: " + line);
			
			/// get the mapper file source
			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			LOG.info(filePath.toString());
			
			for(int i=0; i<line.length(); i++) {
				String sub = line.substring(i, i+1);
				word.set(sub);
				context.write(word, new Text());
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
		                   Context context
		                  ) throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				val.toString();
			}
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();
		
		if (args.length != 2) {
			System.err.println("Usage: SimpleJob <in> <out>");
			return -1;
		}

		LOG.info(conf.get("simple.mr.property"));
		Job job = Job.getInstance(conf, "SimpleJob");
		
		job.setJarByClass(SimpleJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1], String.valueOf(System.currentTimeMillis())));
		
		/// lock file 
		return JobUtils.sumbitJob(job, true, args[0]) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new SimpleJob(), args);
		System.exit(res);
	}
}
