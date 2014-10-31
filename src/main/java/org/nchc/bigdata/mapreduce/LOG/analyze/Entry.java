package org.nchc.bigdata.mapreduce.LOG.analyze;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 1403035 on 2014/10/6.
 */
public class Entry implements WritableComparable<Entry> {

    private Text _Class;
    private Text _Exception;

    public Entry(){
        _Class = new Text();
        _Exception = new Text();
    }

    public Entry(String _class, String _exception){
        _Class = new Text(_class);
        _Exception = new Text(_exception);
    }

    public void setClass(String c){
        _Class.set(c);
    }

    public void setException(String e){
        _Exception.set(e);
    }
    @Override
    public int compareTo(Entry o) {
        int cmp = _Class.compareTo(o._Class);
        if (cmp != 0){
            return cmp;
        }
        return _Exception.compareTo(o._Exception);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        _Class.write(out);
        _Exception.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _Class.readFields(in);
        _Exception.readFields(in);
    }


    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "("+_Class+","+_Exception+")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Entry){
            Entry r = (Entry)obj;
            return _Class.equals(r._Class) && _Exception.equals(r._Exception);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return _Class.hashCode() * 163 + _Exception.hashCode();
    }
}
