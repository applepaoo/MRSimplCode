package tw.com.ruten.ts.localtool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tw.com.ruten.ts.utils.TsConf;

public class SeqfileReader extends Configured implements Tool {

	public void showUsage(){
		System.err.println("SeqfileReader <path>" );
	}
	
	List<Text> listFields = new ArrayList<Text>(); 
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 1){
			showUsage();
			return -1;
		}
		
		String pathStr = args[0];
		
		for(int i=1; i<args.length; i++) {
			if("-listField".equals(args[i])) {
				listFields.add(new Text(args[++i])); 
			}
		}

		FileSystem fs = FileSystem.get(getConf());
		
		Path path = new Path(pathStr);
		if(fs.isDirectory(path)) {
			RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
			
			while(iterator.hasNext()) {
				LocatedFileStatus fileStatus = iterator.next();
				
				if(fileStatus.getPath().toString().endsWith("_SUCCESS")) { /// skip
					continue;
				}
				
				readPath(fileStatus.getPath());
			}
			
			
		}else {
			readPath(path);
		}

        
		return 0;
	}
	
	public void readPath(Path path) throws IllegalArgumentException, IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(getConf(), 
				SequenceFile.Reader.file(path),
				SequenceFile.Reader.bufferSize(4096), 
				SequenceFile.Reader.start(0)
	);
	
	
	Writable key = (Writable) ReflectionUtils.newInstance(
			reader.getKeyClass(), getConf());
    Writable value = (Writable) ReflectionUtils.newInstance(
    		reader.getValueClass(), getConf());
	
    
    while(reader.next(key, value)){
    	StringBuilder strBuilder = new StringBuilder();
    	if(value instanceof MapWritable) {
        	MapWritable map = (MapWritable) value;
        	
        	
        	if(listFields.size() > 0) {
        		for(Text field: listFields) {
        			if(strBuilder.length() > 0){
        				strBuilder.append('\t');
        			}
        			strBuilder.append(toString(map.get(field)));
        		}
        	}else {
        		Set<Writable> keySet = map.keySet();
	        	for(Writable k: keySet){
	        		if(strBuilder.length() > 0){
	        			strBuilder.append('\t');
	        		}
	        		strBuilder.append(toString(k)).append("=").append(toString(map.get(k)));
	        	}
        	}
        	
        	System.out.println(key.toString() +"\t" + strBuilder.toString());
    	}else {
    		System.out.println(key.toString() +"\t" + value.toString());
    	}
    }
	
    reader.close();
	}
	
	public static String toString(Writable in){
		if(in == null){
			return "";
		}
		
		if(in instanceof MapWritable){
			StringBuilder strBuilder = new StringBuilder();
			MapWritable map = (MapWritable) in;
			Set<Writable> keySet = map.keySet();
			for(Writable key: keySet){
				Writable value = map.get(key);
				if(strBuilder.length() > 0){
					strBuilder.append(",");
				}
				
				strBuilder.append(toString(key))
					.append(":")
					.append(toString(value));
			}
				
			return strBuilder.toString();
		}
		
		return  in.toString();
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(TsConf.create(), new SeqfileReader(), args);
		System.exit(res);
	}

}
