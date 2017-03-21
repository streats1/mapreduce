package com.bit2017.mapreduce;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.bit2017.mapreduce.io.NumberWritable;
import com.bit2017.mapreduce.io.StringWritable;


public class WordCount {
	
private static Log log = LogFactory.getLog(WordCount.class);

	public static class MyMapper extends Mapper<LongWritable,Text, StringWritable,NumberWritable> {
		private StringWritable word = new StringWritable(); 
		private static NumberWritable one = new NumberWritable(1L); 
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line,"\r\n\t,|()<> ''.:");
			while(tokenize.hasMoreTokens()){
			word.set(tokenize.nextToken().toLowerCase());
			context.write(word,one); 
			}
			}
		
	}

	public static class MyReducer extends Reducer<StringWritable,NumberWritable,StringWritable ,NumberWritable> {

		private NumberWritable sumWritable = new NumberWritable();
		
		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values,
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(NumberWritable value : values){
			
			sum += value.get();
			
		}
		sumWritable.set(sum);
		context.write(key, sumWritable);
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"WordCount");
		//1.job instance 초기화작업
		job.setJarByClass(WordCount.class);
		
		//2.mapper class 지정
		job.setMapperClass(MyMapper.class);
		//3.리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		
		//4.출력키
		job.setMapOutputKeyClass(StringWritable.class);
		//5출력 밸류ㅜ
		job.setMapOutputValueClass(LongWritable.class);
		
		//6입력파일 포멧 지정(생략가능)
		job.setInputFormatClass(TextInputFormat.class);
		//7 출력파일 포맷 ㅈㅣ정.(생략가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));	
		//9풀력파일 이름지정
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//실행
		job.waitForCompletion(true);
	}
}
