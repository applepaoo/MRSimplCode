package tw.com.ruten.ts.tool;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;


/*
 * @author : realmeat@staff.ruten.com.tw
 */

public class Seq2JsonJob extends Configured implements Tool{
	public static Logger LOG = Logger.getLogger(Seq2JsonJob.class);
	public Configuration conf;

	public static class JsonReducer extends Reducer<Text,MapWritable,Text, NullWritable> {
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		}
		
		public void reduce(Text key, Iterable<MapWritable> values,
		                   Context context
		                  ) throws IOException, InterruptedException {
			for(MapWritable value: values) {
				JSONObject jsonObject = new JSONObject();
					
					
				Set<Writable> keySet = value.keySet();
				for(Writable k:keySet) {
						
					Object v = value.get(k);
					if(v instanceof LongWritable) {
						jsonObject.put(k.toString(), ((LongWritable)v).get());
					}else {
						jsonObject.put(k.toString(), v.toString());
					}
				}
				
				context.write(new Text(jsonObject.toJSONString()), NullWritable.get());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: SellerCheckJob <in> <out>");
			return -1;
		}
		conf = getConf();
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		Job job = Job.getInstance(conf, "seller check list");
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, in);
		
		job.setJarByClass(Seq2JsonJob.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setReducerClass(JsonReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, out);
		
		/// lock file 
		return JobUtils.sumbitJob(job, true) ? 0 : -1;	
	}
	
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new Seq2JsonJob(), args);
		System.exit(res);
	}
}
