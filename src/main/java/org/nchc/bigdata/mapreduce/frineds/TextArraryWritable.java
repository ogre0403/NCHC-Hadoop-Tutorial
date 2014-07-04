package org.nchc.bigdata.mapreduce.frineds;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArraryWritable extends ArrayWritable {
	public TextArraryWritable() {
		super(Text.class);
	}

	public TextArraryWritable(Text[] data) {
		super(Text.class, data);
	}

	@Override
	public int hashCode() {
		return 123456;
	}

	@Override
	public String toString() {
		Text[] aa = (Text[])this.toArray();
		StringBuilder b = new StringBuilder();
		for(int i =0;i<aa.length;i++){
			b.append(aa[i].toString());
			if (i == aa.length -1)
				break;
			b.append(",");
		}
		return b.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextArraryWritable){
			TextArraryWritable aa = (TextArraryWritable)obj;
			Text[] a = (Text[])aa.toArray();
			Text[] b = (Text[])this.toArray();
			return Arrays.equals(a,b);
		}
		
		return false;
	}
}
