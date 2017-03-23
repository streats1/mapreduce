package com.bit2017.mapreduce.trigram;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.topn.TopN;
 

public class Trigram { 

	public static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable> {
		private Text trigram = new Text(); 
		private static LongWritable one = new LongWritable(1L); //?
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,
				LongWritable>.Context context)//context 정확한 의미알기
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line,"\r\n\t,/|()<> ''.:");
			if(tokenize.countTokens() >= 3) { 
				String firstToken = tokenize.nextToken().toLowerCase();
				String secondToken = tokenize.nextToken().toLowerCase();
			while(tokenize.hasMoreTokens()){
			String thirdToken = tokenize.nextToken().toLowerCase();
			
			//지금까지 읽은 3개의 단어로 trigram을 만들고 그걸 키로 해서 출력 레코드를 만든다
			trigram.set(firstToken + " " + secondToken + " " + thirdToken);
			
			context.write(trigram,one);  
			//firstToken 과 secondToken 을 엡데이트 한다.
			firstToken = secondToken;
			secondToken = thirdToken;
			}
			}
		}
	
	}
	public static class MyReducer extends Reducer<Text,LongWritable,Text ,
	LongWritable> {//받는것과 나가는 클래스가 같은것,.

		private LongWritable sumWritable = new LongWritable();//왜 숫자클래스만 객체생성??
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException 
		{
		long sum = 0;
		for(LongWritable value : values){
			
			sum += value.get();//중복확인.
			
		}
		sumWritable.set(sum);
		
		context.getCounter("Word Status","Count of all Words").increment(sum);//이게 왜 필요한지.
		context.write(key, sumWritable);//what?word
		}
		//중복 골라내는 거 알아보기.......................
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"Trigram");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(Trigram.class);//
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
		job.setInputFormatClass(TextInputFormat.class);
		//7 출력파일 포맷 ㅈㅣ정.(생략가능)
		job.setOutputFormatClass(TextOutputFormat.class);//왜 이게 필요한지
		
		
		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));// 어떻게 값이 들어가는지
		//9출력파일 위치지정 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//
		job.waitForCompletion(true);
		
		if(job.waitForCompletion(true)== false){
			return;
		}
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2,"TopN");
		
		job2.setJarByClass(TopN.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		
		job2.setMapperClass(TopN.MyMapper.class);
		job2.setReducerClass( TopN.MyReducer.class );

	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);

		    // input of Job2 is output of Job
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path( args[1] + "/topN"));
	    job2.getConfiguration().setInt( "topN", 10 );

		//실행실행에 필요한 것 
		job.waitForCompletion(true);// .
	}

}

