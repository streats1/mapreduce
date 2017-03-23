package com.bit2017.mapreduce.join;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class JoinIdTitle {

	public static class TitleDocIdMapper extends Mapper<Text,Text, Text,Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(key.toString() + "\t" +1));
		}
	}
	public static class DocIdCiteCountMapper extends Mapper<Text,Text, Text,Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			context.write(key,new Text( value.toString() + "\t" +2));
		}	
	}
	public static class JobIdTitleReducer extends Reducer<Text,Text,Text ,Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			Text k = new Text();
			Text v = new Text();
			
			for(Text value:values){
				String info = value.toString();
				String[] tokens = info.split("\t");// 스트링 타입안에 1.2나눔
				
				if(tokens.length == 2){
					break;
				}
				if("1".equals(tokens)){
					k.set(tokens[0]+"["+key.toString()+"]");
				}else if("2".equals(tokens[1])){
					v.set(tokens[0]);
				}else{
					continue;
				}
				count++;	
			}
			//출력 
			if(count == 2){
				return ;
			}
			
		}

		
	}//받는것과 나가는 클래스가 같은것,.
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"Join Id & Title");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(JoinIdTitle.class);//
		//파라미터 저장
		final String TITLE_ID = args[0];
		final String DOCID_CITECOUNT = args[1];
		final String OUTPUT_DIR = args[2];
		//입력
		MultipleInputs.addInputPath(job,new Path(TITLE_ID),KeyValueTextInputFormat.class,TitleDocIdMapper.class);
		MultipleInputs.addInputPath(job,new Path(DOCID_CITECOUNT),KeyValueTextInputFormat.class,DocIdCiteCountMapper.class);
		//출력 부분
		job.setReducerClass(JobIdTitleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);//왜 이게 필요한지
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_DIR));
		//
		
		//실행
		job.waitForCompletion(true);// .
	}
}