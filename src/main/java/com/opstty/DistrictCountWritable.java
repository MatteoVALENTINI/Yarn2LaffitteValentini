package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictCountWritable implements Writable {
    private Text district;
    private IntWritable count;

    public DistrictCountWritable() {
        this.district = new Text();
        this.count = new IntWritable();
    }

    public DistrictCountWritable(Text district, IntWritable count) {
        this.district = district;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        district.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput);
        count.readFields(dataInput);
    }

    public Text getDistrict() {
        return district;
    }

    public void setDistrict(Text district) {
        this.district = district;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }
}
