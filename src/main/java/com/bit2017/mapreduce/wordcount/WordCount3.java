package com.bit2017.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.bit2017.mapreduce.io.StringWritable;

public class WordCount3 {
/*	private static Log log = LogFactory.getLog(WordCount.class);
*/
	public static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable> {
		private Text word = new Text(); 
		private static LongWritable one = new LongWritable(1L); 
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			 
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line,"\r\n\t,|()<> ''.:");
			while(tokenize.hasMoreTokens()){
			word.set(tokenize.nextToken().toLowerCase());
			context.write(word,one); 
			line.contains(line);
			}
			}
		
	}

	public static class MyReducer extends Reducer<Text,LongWritable,Text ,LongWritable> {

		private LongWritable sumWritable = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable >.Context context) throws IOException, InterruptedException {
			long sum = 0;
		for(LongWritable value : values){
			
			sum += value.get();
			
		}
			
		sumWritable.set(sum);
		context.getCounter("Word Status","Count of all Words").increment(sum);
		context.getCounter("Word Status","Count of unique Words").increment(1);
	/*	context.get*/
		context.write(key, sumWritable);
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"WordCount2");
		//1.job instance 초기화작업
		job.setJarByClass(WordCount3.class);
		
		//2.mapper class 지정
		job.setMapperClass(MyMapper.class);
		//3.리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks( 2 );
	/*	job.setCombinerClass(MyReducer.class);*/
		
		//4.출력키
		job.setMapOutputKeyClass(Text.class);
		//5출력 밸류ㅜ
		job.setMapOutputValueClass(LongWritable.class);
		
		//6입력파일 포멧 지정
		job.setInputFormatClass(TextInputFormat.class); 
		//7 출력파일 포맷 ㅈㅣ정.(생략가능)
		job.setOutputFormatClass(TextOutputFormat.class);

		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));	// 이게 정확히 뭔지.
		//9풀력파일 이름지정
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//실행
		job.waitForCompletion(true);
	}
}


