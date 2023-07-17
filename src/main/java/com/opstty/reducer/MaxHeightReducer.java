package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxHeight = 0;

        for (DoubleWritable val : values) {
            if (val.get() > maxHeight) {
                maxHeight = val.get();
            }
        }

        context.write(key, new DoubleWritable(maxHeight));
    }
}
