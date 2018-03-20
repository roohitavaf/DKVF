package edu.msu.cse.accf.server;

import java.util.ArrayList;
import java.util.List;


import edu.msu.cse.dkvf.metadata.Metadata.TgTimeItem;

public class Utils {
	static List<Long> max (List<Long> first, List<Long> second){
		List<Long> result = new ArrayList<>();
		for (int i=0; i < first.size(); i++){ 
			result.set(i, Math.max(first.get(i), second.get(i)));
		}
		return result;
	}
	
	
	static List<Long> min (List<Long> first, List<Long> second){
		List<Long> result = new ArrayList<>();
		for (int i=0; i < first.size(); i++){ 
			result.set(i, Math.min(first.get(i), second.get(i)));
		}
		return result;
	}
	
	static long getPhysicalTime (){
		return System.currentTimeMillis();
	}
	
	
	static long max (List<TgTimeItem> ds){
		long result = ds.get(0).getTime(); 
		for (int i = 1; i < ds.size(); i++)
			if (result < ds.get(i).getTime()) 
				result = ds.get(i).getTime();
		return result;
	}
	
	//HLC operations
	public static long getL(long time) {
		return time & 0xFFFFFFFFFFFF0000L;
	}

	public static long getC(long time) {
		return time & 0x000000000000FFFFL;
	}

	public static long incrementL(long time) {
		return shiftToHighBits(1) + time;
	}


	public static long shiftToHighBits(long time) {
		return time << 16;
	}
	
	public static long maxDsTime(List<TgTimeItem> tgItemList) {
		if (tgItemList == null || tgItemList.isEmpty())
			return 0;
		long result = tgItemList.get(0).getTime();
		for (int i = 1; i < tgItemList.size(); i++)
			if (result < tgItemList.get(i).getTime())
				result = tgItemList.get(i).getTime();
		return result;
	}
}
