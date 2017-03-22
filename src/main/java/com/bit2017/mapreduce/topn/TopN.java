package com.bit2017.mapreduce.topn;

import java.io.IOException;
import java.util.PriorityQueue;

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


public class TopN {

	public static class MyMapper extends Mapper<Text,Text, Text,LongWritable> {//?????????????????????????
	private int topN =10;
	private PriorityQueue<ItemFreq> pq = null;
	@Override
	protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)//역할
			throws IOException, InterruptedException {
		topN = context.getConfiguration().getInt("topN",10);// max  10개를 지정
		pq =new PriorityQueue<ItemFreq>(10,new ItemFreqComparator());
		
	}
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		ItemFreq newItemFreq = new ItemFreq();
		newItemFreq.setItem(key.toString());
		newItemFreq.setFreq(Long.parseLong(value.toString()));
		
		ItemFreq head = pq.peek();
		if(pq.size()< topN || head.getFreq() < newItemFreq.getFreq()){ //큰숫자를 넣는 이프문
			pq.add(newItemFreq);
			if(pq.size() > topN){// 10개 이상이 되면 알아서 지워라
				pq.remove();
			}
			
		}
	}
	@Override
	protected void cleanup(Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		while(pq.isEmpty() == false){// 값이 없을때 까지 
		ItemFreq itemfreq=	pq.remove();//값을 빼달라는 의미.
		context.write(new Text(itemfreq.getItem()),new LongWritable(itemfreq.getFreq()));
		}
	}
}

	public static class MyReducer extends Reducer<Text,LongWritable,Text ,
	LongWritable> {//받는것과 나가는 클래스가 같은것,.
		private int topN =10;//디폴트값
		private PriorityQueue<ItemFreq> pq = null;
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException 
		{
			
			Long sum = 0L;
			for(LongWritable value:values){
				sum += value.get();//중복골라주고
				
			}
			ItemFreq newItemFreq = new ItemFreq();
			newItemFreq.setItem(key.toString());//택스트 나열
			newItemFreq.setFreq(sum);
			
			ItemFreq head = pq.peek();
			if(pq.size()<topN || head.getFreq() < newItemFreq.getFreq()){ // new 기본
				pq.add(newItemFreq);
				if(pq.size() > topN){
					pq.remove();
				}
			
		}
		}
		@Override
		protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt("topN",10);
			pq =new PriorityQueue<ItemFreq>(10,new ItemFreqComparator());
		}
		@Override
		protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)//정리.
				throws IOException, InterruptedException {
			while(pq.isEmpty() == false){
			ItemFreq itemfreq=	pq.remove();
			context.write(new Text(itemfreq.getItem()),new LongWritable(itemfreq.getFreq()));
		}
		}
		}

	

	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();//이것과
		Job job = new Job(conf,"TopN");//이것의 의미!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 꼭물어보기
		//1.job instance 초기화작업
		job.setJarByClass(TopN.class);//
		
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
		//n 파라미터
		job.getConfiguration().setInt("TopN",Integer.parseInt(args[2]));
		
		//실행실행에 필요한 것
		job.waitForCompletion(true);// .
	}


	}


