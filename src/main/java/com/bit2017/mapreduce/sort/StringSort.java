package com.bit2017.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class StringSort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"String Sort");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//job instance 초기화작업
		job.setJarByClass(StringSort.class);//
		
		//mapper class 지정
		job.setMapperClass(Mapper.class);
		//리듀서 클래스 지정
		job.setReducerClass(Reducer.class);
		//리튜서 타스크 갯수
		
		//4.맵 풀력 키 타입
		job.setMapOutputKeyClass(Text.class);
		//5맵 출력  밸류 타입ㅜ
		job.setMapOutputValueClass(Text.class);
		
		
		//처리결과 출력 ㅋㅣ 타입(리듀스)
		job.setOutputKeyClass(Text.class);
		//처리결과 출력 밸류 타입(리듀스)
		job.setOutputValueClass(Text.class);

		//입력파일 이름지정(생략가능) 
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 출력파일 디렉토리 지정.(생략가능)
		job.setOutputFormatClass(SequenceFileOutputFormat.class);//왜 이게 필요한지
		
		
		//8 파일 위치 지정
		FileInputFormat.addInputPath(job,new Path(args[0]));// 어떻게 값이 들어가는지
		//9출력파일 위치지정 
		SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		//
		
		//실행실행에 필요한 것
		job.waitForCompletion(true);// .
	}
}
