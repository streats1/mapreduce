package com.bit2017.mapreduce.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CreateESIndex {
	
	
	
	
	public static class ESIdexMapper extends Mapper<Text, Text, Text, Text> {

		private String baseURL = "";
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			String[] host = context.getConfiguration().getStrings("ESServer");
			baseURL = "http://" + host[0] + ":9200/wikipedia/doc/";
		}
		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//URLconnection 객체생성(연결)
			URL url = new URL(baseURL + docId);
			HttpURLConnection urlCon = (HttpURLConnection)url.openConnection();
			urlCon.setDoOutput(true);
			urlCon.setRequestMethod("PUT");//연결 해더 완성
			
			//json 문자열 만들기.
			String line = contents.toString().replace("\\","\\\\").replace("\"","\\\"");
			String json = "{\"body\":\"" + line + "\"}";
		
			//데이터 보내기.
			OutputStreamWriter out = new OutputStreamWriter(urlCon.getOutputStream());//네트웍 소켓 연결 구문
			out.write(json);//byte 단위 바뀌면서 씀 (보조 sTREAM)
			out.close();//(request 한거임.)
			
			//응답받기
			String lines= "";
			BufferedReader br = new BufferedReader(new InputStreamReader(urlCon.getInputStream()));
			while((line = br.readLine()) != null){
				line += line ;
				
			}
			//결과처리
			if(lines.indexOf( "\"successful\":1,\"failed\":0" ) < 0){
				//실패
				context.getCounter("Index stats","fail").increment(1);
			}else{
				context.getCounter("Index stats","success").increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"Create ES Index");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		job.setJarByClass(CreateESIndex.class);//
		
		job.setMapperClass(ESIdexMapper.class);
		/*job.setReducerClass(MyReducer.class);*/
		job.setNumReduceTasks( 0 );
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));// 어떻게 값이 들어가는지
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.getConfiguration().setStrings("ESServer",args[2]);// 하는일 알아보기
		job.waitForCompletion(true);// .
	}
}
