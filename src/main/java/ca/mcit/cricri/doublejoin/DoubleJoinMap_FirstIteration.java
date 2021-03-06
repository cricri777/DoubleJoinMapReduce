package ca.mcit.cricri.doublejoin;

import java.io.IOException;

import static ca.mcit.cricri.doublejoin.model.DataUtiliy.SEPARATOR;
import static ca.mcit.cricri.doublejoin.model.DataUtiliy.FILE_NAME_R;
import static ca.mcit.cricri.doublejoin.model.DataUtiliy.FILE_NAME_S;
import static ca.mcit.cricri.doublejoin.model.DataUtiliy.FILE_NAME_T;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

@Slf4j
public class DoubleJoinMap_FirstIteration extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        log.trace("incoming mapping from filename:[{}], key: [{}], value: [{}]", fileName, key, value.toString());

        String record = value.toString();
        String[] fields = record.split(SEPARATOR);

        String value1 = fields[0];
        String value2 = fields[1];

        Text keyOut;
        Text valueOut;
        switch (fileName) {
            case FILE_NAME_R:
                keyOut = new Text(value2.toUpperCase());
                valueOut = new Text(fileName + SEPARATOR + record);
                context.write(keyOut, valueOut);
                break;

            case FILE_NAME_S:
                keyOut = new Text(value1.toUpperCase());
                valueOut = new Text(fileName + SEPARATOR + record);
                context.write(keyOut, valueOut);
                break;

            case FILE_NAME_T:
                //do nothing for the first iteration.
                break;
            default:
                log.info("hadoop file name does not follow convention, should be [{}], [{}] or [{}] but was [{}]", FILE_NAME_R, FILE_NAME_S, FILE_NAME_T, fileName);
        }
    }

}