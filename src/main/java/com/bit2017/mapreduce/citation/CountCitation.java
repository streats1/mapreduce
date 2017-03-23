package com.bit2017.mapreduce.citation;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CountCitation {
	

	public static class MyMapper extends Mapper<Text,Text, Text,LongWritable> {

		private LongWritable one = new LongWritable(1L);
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, one);
		}
		
	
			}
		
	

	public static class MyReducer extends Reducer<Text,LongWritable,Text ,
	LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			
			Long sum = 0L;
			for(LongWritable value:values){
				sum += value.get();
			}
			context.write(key,new LongWritable(sum));
		}//받는것과 나가는 클래스가 같은것,.

		
		//중복 골라내는 거 알아보기.......................
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"Count citation");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(CountCitation.class);//
		
		//2.mapper class 지정
		job.setMapperClass(MyMapper.class);
		//3.리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		//리튜서 타스크 갯수
		
		//4.출력키
		job.setMapOutputKeyClass(Text.class);
		//5출력 밸류ㅜ
		job.setMapOutputValueClass(LongWritable.class);
		
		//6입력파일 포멧 지정(생략가능) 
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//7 출력파일 포맷 ㅈㅣ정.(생략가능)
		job.setOutputFormatClass(TextOutputFormat.class);//왜 이게 필요한지
		
		
		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));// 어떻게 값이 들어가는지
		//9출력파일 위치지정 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//
		
		//실행실행에 필요한 것
		job.waitForCompletion(true);// .
	}
}
