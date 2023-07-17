package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TallestTreeMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text species = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");

        // Getting tree species
        String treeSpecies = fields[3];

        // Getting tree height
        String treeHeightStr = fields[6];

        try {
            double treeHeight = Double.parseDouble(treeHeightStr);
            species.set(treeSpecies);
            context.write(species, new DoubleWritable(treeHeight));
        } catch (NumberFormatException e) {
            // Ignore if the height can't be parsed to a double
        }
    }
}
