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
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable,
				NumberWritable>.Context context)//context 정확한 의미알기
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line,"\r\n\t,|()<> ''.:");
			while(tokenize.hasMoreTokens()){
			word.set(tokenize.nextToken().toLowerCase());
			context.write(word,one); 
			}
			}
		
	}

	public static class MyReducer extends Reducer<StringWritable,NumberWritable,StringWritable ,
	NumberWritable> {//받는것과 나가는 클래스가 같은것,.

		private NumberWritable sumWritable = new NumberWritable();//왜 숫자클래스만 객체생성??
		
		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values,
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(NumberWritable value : values){
			
			sum += value.get();//전체 갯수.
			
		}
		sumWritable.set(sum);
		
		context.getCounter("Word Status","Count of all Words").increment(sum);
		context.write(key, sumWritable);//what?
		}
		//중복 골라내는 거 알아보기.......................
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"WordCount");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(WordCount.class);//
		
		//2.mapper class 지정
		job.setMapperClass(MyMapper.class);
		//3.리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		//리튜서 타스크 갯수
		job.setNumReduceTasks( 2 );
		job.setCombinerClass(MyReducer.class);
		//4.출력키
		job.setMapOutputKeyClass(StringWritable.class);
		//5출력 밸류ㅜ
		job.setMapOutputValueClass(NumberWritable.class);
		
		//6입력파일 포멧 지정(생략가능)
		job.setInputFormatClass(TextInputFormat.class);
		//7 출력파일 포맷 ㅈㅣ정.(생략가능)
		job.setOutputFormatClass(TextOutputFormat.class);//왜 이게 필요한지
		
		
		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));// 왜만드는지,	무슨 파일을 말하는지
		//9출력파일 위치지정 (/input/README.txt 이것이 인풋 /wordcount-result19요것이 아웃풋 그래서 저 잡 인아웃풋이 필요함)
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//
		
		//실행실행에 필요한 것
		job.waitForCompletion(true);// .
	}
}
