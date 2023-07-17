package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text species = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        String treeSpecies = fields[3];
        String treeHeight = fields[4];

        try {
            double height = Double.parseDouble(treeHeight);
            species.set(treeSpecies);
            context.write(species, new DoubleWritable(height));
        } catch (NumberFormatException e) {
            // Ignore if the height cannot be parsed
        }
    }
}
