package com.bit2017.mapreduce.pq;

import java.util.PriorityQueue;

public class TestMain {

	public static void main(String[] args) {
		PriorityQueue<String> pq = new PriorityQueue(10,new StringConparator());
		pq.add("");
		pq.add("hello");
		pq.add("hello");
		pq.add("hello world");
		pq.add("hi");
		pq.add("shut");
		pq.add("asdfghjkzxcvbnm");

		while(pq.isEmpty() == false){
			String s = pq.remove();
			System.out.println(s);
		}
	}
}
