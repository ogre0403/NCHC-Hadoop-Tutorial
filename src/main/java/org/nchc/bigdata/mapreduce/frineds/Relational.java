package org.nchc.bigdata.mapreduce.frineds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Relational implements WritableComparable<Relational> {

	private Text A;
	private Text B;
	
	
	public Relational(){
		A = new Text();
		B = new Text();
	}
	
	public Relational(String a, String b){
		A = new Text(a);
		B = new Text(b);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		A.write(out);
		B.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		A.readFields(in);
		B.readFields(in);
	}

	public int compareTo(Relational o) {
		int cmp = A.compareTo(o.A);
		if (cmp != 0){
			return cmp;
		}
		return B.compareTo(o.B);
	}

	@Override
	public int hashCode() {
		return A.hashCode() * 163 + B.hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return A+":"+B;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Relational){
			Relational r = (Relational)obj;
			return A.equals(r.A) && B.equals(r.B);  
		}
		return false;
	}

}
