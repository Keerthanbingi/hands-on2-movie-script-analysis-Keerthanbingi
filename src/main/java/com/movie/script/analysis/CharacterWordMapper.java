package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text characterWord = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Example of input line format: "CHARACTER_NAME: dialogue"
        String line = value.toString();
        int colonIndex = line.indexOf(':');
        if (colonIndex > 0) {
            String character = line.substring(0, colonIndex).trim();
            String dialogue = line.substring(colonIndex + 1).trim();

            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                characterWord.set(character + "\t" + word.toString());
                context.write(characterWord, one);
            }
        }
    }
}
