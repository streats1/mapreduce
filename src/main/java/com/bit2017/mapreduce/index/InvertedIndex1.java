package com.bit2017.mapreduce.index;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InvertedIndex1 {

	public static class MyMapper extends Mapper<Text,Text, Text,Text> {
		private Text word = new Text(); 
		private static LongWritable one = new LongWritable(1L); //?
		

		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text,
				Text>.Context context)//context 정확한 의미알기
				throws IOException, InterruptedException {
			
			String line = contents.toString();
			StringTokenizer tokenize = new StringTokenizer(line,"\r\n\t,|()<> ''.:");
			while(tokenize.hasMoreTokens()){
			word.set(tokenize.nextToken().toLowerCase());//word 
			context.write(word,docId);  
			}
			}
		
	

	public static class MyReducer extends Reducer<Text,Text,Text ,
	Text> {//받는것과 나가는 클래스가 같은것,.

		private LongWritable sumWritable = new LongWritable();//왜 숫자클래스만 객체생성??
		
		@Override
		protected void reduce(Text word, Iterable<Text> docIds,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
		String s ="";	
		boolean isFirst = true;
		for(Text docId : docIds){
			if(isFirst == true){
				s +=",";
			}else{
				isFirst = false;
			}
			s += docId.toString();//중복확인.
			
		}
		context.write(word,new Text(s));//what?word
		}
		//중복 골라내는 거 알아보기.......................
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"Inverted Index1");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(InvertedIndex1.class);//
		
		//2.mapper class 지정
		job.setMapperClass(MyMapper.class);
		//3.리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		//리튜서 타스크 갯수
		
		//4.출력키
		job.setMapOutputKeyClass(Text.class);
		//5출력 밸류ㅜ
		job.setMapOutputValueClass(Text.class);
		
		//4.reduce 출력키
		job.setMapOutputKeyClass(Text.class);
				//reduce 출력 밸류ㅜ
		job.setMapOutputValueClass(Text.class);
		
		
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
}