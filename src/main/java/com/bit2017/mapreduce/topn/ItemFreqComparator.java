package com.bit2017.mapreduce.topn;

import java.util.Comparator;

public class ItemFreqComparator  implements Comparator<ItemFreq> {

	@Override
	public int compare(ItemFreq o1, ItemFreq o2) {
	if(o1.getFreq() == o2.getFreq()){
		return 0;//같은수!!!
	}
	if(o1.getFreq() > o2.getFreq()){
		return 1;//1 이면 기존보다 큰수
	}
	return -1;
	}

}
