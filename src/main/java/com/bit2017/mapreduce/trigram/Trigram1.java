package com.bit2017.mapreduce.trigram;

import java.util.StringTokenizer;

public class Trigram1 {
	public static void main(String[] args) {
		String s = "the Art of Map Reduce Programming";
		trigram( s );

		System.out.println( "==================================");

		s = "the Art";
		trigram( s );
	}

	public static void trigram( String s ) {
		StringTokenizer tokenizer = new StringTokenizer( s, "\r\n\t,|()<> ''.:" );
		
		String firstWord = tokenizer.nextToken();
		String secondWord = tokenizer.nextToken();
		while( tokenizer.hasMoreTokens() ) {
			String thirdWord = tokenizer.nextToken();
			
			String trigram = firstWord + " " + secondWord + " " + thirdWord;
			System.out.println( trigram );
			
			firstWord = secondWord;
			secondWord = thirdWord;
		}
	}
}
