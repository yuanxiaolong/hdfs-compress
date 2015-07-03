package com.yxl.hadoop;

import com.yxl.hadoop.util.PropertyUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

import java.io.InputStreamReader;

/**
 * 用 hadoop lzo 算法压缩 hdfs 文件并以 SequenceFile 存储
 * 不是mr的方式,是hadoop api 的方式
 * <p/>
 * Created by yuanxiaolong on 15/6/26.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";

    private static String FS = "hdfs://localhost:8020";

    private static String HDFS_SCHEMA = "hdfs://";

    private static String HELP_MSG = "";

    private static final String LZO_SUFFIX = ".lzo";


    static {
        FS = PropertyUtil.getInstance().getProperty("fs.defaultFS", FS);
        if (LOG.isDebugEnabled()) {
            LOG.info("gloobal.properties [fs.defaultFS]:");
        }

        HELP_MSG = "--------------------------------------------------------------------------------------------" + "\n";
        HELP_MSG += "please input cmd 'hadoop jar hadoop-lzo-0.0.1.jar <input hdfs path> <output hdfs path> '" + "\n";
        HELP_MSG += "-input:\t\t <hdfs path prepare compress dir or file> " + "\n";
        HELP_MSG += "-output:\t\t<hdfs path compressed must be single dir> " + "\n";
        HELP_MSG += "--------------------------------------------------------------------------------------------";

    }


    /**
     * 入口函数,抛出任何异常
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        System.out.println("[start] application");

        if (args != null && args.length < 2) {
            System.out.println(HELP_MSG);
            System.out.println("[stop] application");
            return;
        }

        String in = "";
        String out = "";
        if (!StringUtils.startsWithIgnoreCase(args[0],HDFS_SCHEMA)){
            in = FS + args[0];
        }
        if (!StringUtils.startsWithIgnoreCase(args[1],HDFS_SCHEMA)){
            out = FS + args[1];
        }

        compress(in, out);

        System.out.println("[end] application");

    }


    private static void compress(String inputPath, String outputPath) {
        System.out.println("[input]: " + inputPath);
        System.out.println("[output]: " + outputPath);

        if (StringUtils.isBlank(inputPath) || StringUtils.isBlank(outputPath)){
            System.out.println("[input] or [output] must not be blank");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", FS);

        try {
            FileSystem fs = FileSystem.get(conf);
            Path input = new Path(inputPath);

            // 1. check input path exist
            if (!fs.exists(input)) {
                System.out.println("input path not exist");
                return;
            }

            // 2. check output path must a dir
            if (!StringUtils.endsWith(outputPath,"/")){
                System.out.println("output path must be a dir , end with '/' ");
                return;
            }

            // 3. check input path is dir ,if it is ,foreach write
            FileStatus stat = fs.getFileStatus(input);
            if (stat.isFile()) {
                write(fs,conf,stat,outputPath);
            } else if (stat.isDirectory()) {
                FileStatus[] subInputFile = fs.listStatus(input);
                for (FileStatus fileStatus : subInputFile) {
                    if (fileStatus.isFile()) {
                        write(fs,conf,fileStatus,outputPath);
                    } else {
                        System.out.println("input path must be iter by once");
                        break;
                    }
                }
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    private static void write(FileSystem fs, Configuration conf,FileStatus fileStatus,String outputPath) throws IOException{
        String in = fileStatus.getPath().toString();
        String out = outputPath + fileStatus.getPath().getName() + LZO_SUFFIX;
        checkParentDir(fs, out);
        System.out.println(in + "---->" + out);
        writeFile(fs, conf, in, out);
    }


    private static void checkParentDir(FileSystem fs, String out) throws IOException {
        Path parent = new Path(out).getParent();
        if (!fs.exists(parent)) {
            System.out.println("output path dir not exist [" + parent + ",] now mkdir it");
            fs.mkdirs(parent, FsPermission.getDirDefault());
        }
    }


    private static void writeFile(FileSystem fs, Configuration conf, String in, String out) {
        FSDataInputStream inputStream = null;
        SequenceFile.Writer writer = null;
        try {
            inputStream = fs.open(new Path(in));
            Path seqFile = new Path(out);

            BufferedReader buff = new BufferedReader(new InputStreamReader(inputStream));

            BytesWritable EMPTY_KEY = new BytesWritable();//   key

            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(LZO), conf);
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(seqFile), SequenceFile.Writer.keyClass(BytesWritable.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

            String str = "";
            System.out.println("begin write " + out);
            while ((str = buff.readLine()) != null) {
                writer.append(EMPTY_KEY, new Text(str));
            }
            System.out.println("done write " + out);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(writer);
        }
    }


}
