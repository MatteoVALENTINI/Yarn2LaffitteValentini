package com.opstty.mapper;

import com.opstty.TreeAgeDistrictWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, LongWritable, TreeAgeDistrictWritable> {
    private LongWritable year = new LongWritable();
    private TreeAgeDistrictWritable treeAgeDistrictWritable = new TreeAgeDistrictWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");

        String districtStr = fields[1];
        String yearStr = fields[5];

        try {
            int district = Integer.parseInt(districtStr);
            long plantingYear = Long.parseLong(yearStr);

            treeAgeDistrictWritable.set((int) (System.currentTimeMillis() / 1000L / 60 / 60 / 24 / 365.25 - plantingYear), district);
            year.set(plantingYear);
            context.write(year, treeAgeDistrictWritable);
        } catch (NumberFormatException e) {

        }
    }
}
