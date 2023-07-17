package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeAgeDistrictWritable implements Writable {
    private int age;
    private int district;

    public TreeAgeDistrictWritable() {
    }

    public TreeAgeDistrictWritable(int age, int district) {
        set(age, district);
    }

    public void set(int age, int district) {
        this.age = age;
        this.district = district;
    }

    public int getAge() {
        return age;
    }

    public int getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(age);
        out.writeInt(district);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age = in.readInt();
        district = in.readInt();
    }
}
