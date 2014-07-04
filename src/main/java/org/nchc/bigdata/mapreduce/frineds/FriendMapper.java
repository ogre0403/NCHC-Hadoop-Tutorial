package org.nchc.bigdata.mapreduce.frineds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FriendMapper extends
		Mapper<Object, Text, Relational, TextArraryWritable> {

	private static Logger logger = Logger.getLogger(FriendMapper.class);
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] strings = value.toString().split("\t");
		String man = strings[0].trim();

		String[] friend = strings[1].trim().split(",");

		TextArraryWritable aw = new TextArraryWritable(getFriends(strings[1]));

		for (int i = 0; i < friend.length; i++) {
			Relational r;
			if (man.charAt(0) < friend[i].charAt(0)) {
				r = new Relational(man, friend[i]);
			} else {
				r = new Relational(friend[i], man);
			}
			logger.info(r+"===="+aw);
			
			context.write(r, aw);
		}

	}

	private Text[] getFriends(String value) {
		String[] friend = value.trim().split(",");
		Text[] ta = new Text[friend.length];

		for (int i = 0; i < friend.length; i++) {
			ta[i] = new Text(friend[i]);
		}

		return ta;
	}

}
