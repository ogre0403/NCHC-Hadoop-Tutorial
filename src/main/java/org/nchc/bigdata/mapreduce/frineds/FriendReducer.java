package org.nchc.bigdata.mapreduce.frineds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class FriendReducer extends Reducer<Relational, TextArraryWritable,Relational, TextArraryWritable> {

	private static Logger logger = Logger.getLogger(FriendReducer.class);
	@Override
	protected void reduce(Relational key, Iterable<TextArraryWritable> values, Context context)
			throws IOException, InterruptedException {
		
		
		int index = 0;
		Text[][] friend = new Text[2][2];
		
		for (TextArraryWritable val : values) {
			if(index > 1){
				logger.error("not be here");
				return;
			}
			friend[index] = (Text[])val.toArray();
			index++;
		}
		
		if(index == 1){
			return;
		}
		
		List<Text> list0 = new ArrayList<Text>(Arrays.asList(friend[0]));	
		
		List<Text> list1 = new ArrayList<Text>(Arrays.asList(friend[1]));
		list0.retainAll(list1);
		
		int resultsize = list0.size();
		Text[] results = new Text[resultsize];
		
		for(int i=0;i<resultsize;i++){
			results[i] = new Text(list0.get(i));
		}
		
		TextArraryWritable aw = new TextArraryWritable(results);
		context.write(key	, aw);
		
	}

}
