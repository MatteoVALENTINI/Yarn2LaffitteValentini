package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.opstty.DistrictCountWritable;  // Import your custom Writable class

import java.io.IOException;

public class MaxTreeCountReducer extends Reducer<NullWritable, DistrictCountWritable, Text, IntWritable> {

    private DistrictCountWritable maxCount = new DistrictCountWritable();

    public void reduce(NullWritable key, Iterable<DistrictCountWritable> values, Context context) throws IOException, InterruptedException {
        maxCount.setCount(new IntWritable(Integer.MIN_VALUE));

        for (DistrictCountWritable val : values) {
            if (val.getCount().get() > maxCount.getCount().get()) {
                maxCount.setDistrict(val.getDistrict());
                maxCount.setCount(val.getCount());
            }
        }

        context.write(maxCount.getDistrict(), maxCount.getCount());
    }
}
