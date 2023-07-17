package com.opstty.reducer;

import com.opstty.TreeAgeDistrictWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<LongWritable, TreeAgeDistrictWritable, LongWritable, LongWritable> {
    private LongWritable oldestDistrict = new LongWritable();

    public void reduce(LongWritable key, Iterable<TreeAgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        int oldestAge = 0;
        for (TreeAgeDistrictWritable val : values) {
            if(val.getAge() > oldestAge) {
                oldestAge = val.getAge();
                oldestDistrict.set(val.getDistrict());
            }
        }
        context.write(key, oldestDistrict);
    }
}
