package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opstty.DistrictCountWritable;  // Import your custom Writable class

import java.io.IOException;

public class DistrictCountMapper extends Mapper<Object, Text, NullWritable, DistrictCountWritable> {

    private DistrictCountWritable districtCount = new DistrictCountWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        districtCount.setDistrict(new Text(parts[0]));
        districtCount.setCount(new IntWritable(Integer.parseInt(parts[1])));
        context.write(NullWritable.get(), districtCount);
    }
}
