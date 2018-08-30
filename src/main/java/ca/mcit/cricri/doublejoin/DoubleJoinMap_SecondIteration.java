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
public class DoubleJoinMap_SecondIteration extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        log.trace("incoming mapping from filename:[{}], key: [{}], value: [{}]", fileName, key, value.toString());

        String record = value.toString();


        Text keyOut;
        Text valueOut;
        switch (fileName) {
            case FILE_NAME_R:
                break;
            case FILE_NAME_S:
                break;
            case FILE_NAME_T:
                String[] fields = record.split(SEPARATOR);

                String value1 = fields[0];
                keyOut = new Text(value1.toUpperCase());
                valueOut = new Text(fileName + SEPARATOR + record);
                context.write(keyOut, valueOut);
                break;
            case "part-r-00000":
                String[] firstSplit = record.split("\t");
                String newKey = firstSplit[1].split(",")[5];

                valueOut = new Text(fileName + SEPARATOR + firstSplit[1]);
                keyOut = new Text(newKey.toUpperCase());
                context.write(keyOut, valueOut);
                break;
            default:
                log.info("hadoop file name does not follow convention, should be [{}], [{}] or [{}] but was [{}]", FILE_NAME_R, FILE_NAME_S, FILE_NAME_T, fileName);
        }
    }

}