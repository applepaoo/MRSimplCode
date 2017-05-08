package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tw.com.ruten.ts.mapreduce.AdvanceSortJob.SortedKey.GroupComparator;
import tw.com.ruten.ts.mapreduce.AdvanceSortJob.SortedKey.SortComparator;
import tw.com.ruten.ts.utils.TsConf;


/*
 * advance sort example MR project 
 * 
 * @author : realmeat@staff.ruten.com.tw
 */

public class AdvanceSortJob extends Configured implements Tool{

	public static Logger LOG = Logger.getLogger(AdvanceSortJob.class);
	public Configuration conf;
	
	public static class SortedKey implements WritableComparable<SortedKey>{
		LongWritable sortValue = new LongWritable(0);
		Text defaultKey = new Text();

		SortedKey(){}	
		
		
		@Override
		public void readFields(DataInput in) throws IOException {
			defaultKey.set(Text.readString(in));
			sortValue.set(in.readLong());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, defaultKey.toString());
			out.writeLong(sortValue.get());
		}

		@Override
		public int compareTo(SortedKey other) { /// default  
			return this.defaultKey.compareTo(other.defaultKey);
		}
		
		public int sort(SortedKey other){ /// for sort
			int r = this.defaultKey.compareTo(other.defaultKey);
			if(r == 0){
				return this.sortValue.compareTo(other.sortValue);
			}
			
			return r;
		}
		
		public int group(SortedKey other){ /// for group
			return compareTo(other);
		}
		
		@Override
		public int hashCode(){ /// for partition
			return this.defaultKey.toString().hashCode();
		}
	
		public static class SortComparator extends WritableComparator{
			SortComparator(){
				super(SortedKey.class, true);
			}
			
			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				if(o1 instanceof SortedKey && o2 instanceof SortedKey){
					SortedKey k1 = (SortedKey) o1;
					SortedKey k2 = (SortedKey) o2;
					
					return k1.sort(k2);
				}
				
				return o1.compareTo(o2);
			}
		}
		
		public static class GroupComparator extends WritableComparator{
			GroupComparator(){
				super(SortedKey.class, true);
			}
			
			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				if(o1 instanceof SortedKey && o2 instanceof SortedKey){
					SortedKey k1 = (SortedKey) o1;
					SortedKey k2 = (SortedKey) o2;
					
					return k1.group(k2);
				}
				
				return o1.compareTo(o2);
			}
		}
	}
	
	public static class TokenizerMapper  extends Mapper<Object, Text, SortedKey, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Configuration conf;
		private Random random = new Random();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			LOG.info("line: " + line);
			
			for(int i=0; i<line.length(); i++) {
				String sub = line.substring(i, i+1);
				SortedKey outKey = new SortedKey();
				outKey.defaultKey.set(sub);
				IntWritable outValue = new IntWritable(Math.abs(random.nextInt(100)));
				outKey.sortValue.set(outValue.get());
				context.write(outKey, outValue);
			}
		}
	}

	public static class IntSumReducer extends Reducer<SortedKey,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		private Text outKey = new Text();
		public void reduce(SortedKey key, Iterable<IntWritable> values,
		                   Context context
		                  ) throws IOException, InterruptedException {
			LOG.info(key.defaultKey + " :" + key.sortValue );
			int sum = 0;
			for (IntWritable val : values) {
				LOG.info(String.valueOf(val.get()));
				sum+=1;
			}
			result.set(sum);
			outKey.set(key.defaultKey);
			context.write(outKey, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();
		
		if (args.length != 2) {
			System.err.println("Usage: AdvanceSortJob <in> <out>");
			return -1;
		}

		LOG.info(conf.get("simple.mr.property"));
		
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(AdvanceSortJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		
		/// map reduce change 
		job.setMapOutputKeyClass(SortedKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setReducerClass(IntSumReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1], String.valueOf(System.currentTimeMillis())));

		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new AdvanceSortJob(), args);
		System.exit(res);
	}
}
