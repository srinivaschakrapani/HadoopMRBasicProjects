package com.srini.customtypes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Custom class to implement writer
public class JoinWritable implements Writable {
    Text mrValue;
    Text mrFileName;

    public JoinWritable() {
        this(new Text(), new Text());
    }

    public JoinWritable(Text mrValue, Text mrFileName) {
        this.mrValue = mrValue;
        this.mrFileName = mrFileName;
    }

    public Text getMrValue() {
        return mrValue;
    }

    public void setMrValue(Text mrValue) {
        this.mrValue = mrValue;
    }

    public Text getMrFileName() {
        return mrFileName;
    }

    public void setMrFileName(Text mrFileName) {
        this.mrFileName = mrFileName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        mrValue.write(dataOutput);
        mrFileName.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mrValue.readFields(dataInput);
        mrFileName.readFields(dataInput);

    }
}
