package com.conan.bigdata.hadoop.compress;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2019/1/14.
 * Usage Example:
 * hadoop jar hadoop-1.0-SNAPSHOT.jar com.conan.bigdata.hadoop.compress.MultiCompress compress org.apache.hadoop.io.compress.GzipCodec /tmp/repository/compress/2018.12.24.log log gz
 */
public class MultiCompress extends Configured implements Tool {

    class HdfsFile {
        private Path path;
        private String fileDate;
        private String fileName;
        private String fileType;

        public Path getPath() {
            return path;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public void setPath(Path path) {
            this.path = path;
        }

        public String getFileDate() {
            return fileDate;
        }

        public void setFileDate(String fileDate) {
            this.fileDate = fileDate;
        }

        public String getFileType() {
            return fileType;
        }

        public void setFileType(String fileType) {
            this.fileType = fileType;
        }

        @Override
        public String toString() {
            return "[path:" + this.path.toString() + ", fileDate:" + this.fileDate + ", fileType:" + this.fileType + "]";
        }
    }

    private static List<HdfsFile> listFiles = new ArrayList<>();

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getInstance(), new MultiCompress(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * optType     [compress, uncompress]
     * codeClass   [org.apache.hadoop.io.compress.GzipCodec,
     * org.apache.hadoop.io.compress.BZip2Codec,
     * org.apache.hadoop.io.compress.SnappyCodec]
     * sourceDir   [文件]
     * sourceType  [源数据后缀]
     * targetType  [目标数据后缀]
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] argsSplit = new GenericOptionsParser(conf, args).getRemainingArgs();
//        String optType = argsSplit[0];
//        String codeClass = argsSplit[1];
//        String sourceDir = argsSplit[2];
//        String sourceType = argsSplit[3];
//        String targetType = argsSplit[4];
        String dir1 = argsSplit[0];
        String dir2 = argsSplit[1];
//        String fileDate = argsSplit[1];
        String optType = "compress";
        String codeClass = "org.apache.hadoop.io.compress.GzipCodec";
        String sourceDir = String.format("/rdb/%s/%s", dir1, dir2);
        String targetDir = String.format("/rdb_bak/%s/%s", dir1, dir2);
        String sourceType = "log";
        String targetType = "gz";

        if ("compress".equals(optType)) {
            compress(codeClass, conf, sourceDir, targetDir, sourceType, targetType);
//            listFiles(FileSystem.get(conf), new Path(sourceDir));
//            for (HdfsFile file : listFiles) {
//                System.out.println(file);
//            }
            return 0;
        } else if ("uncompress".equals(optType)) {
            return 0;
        } else {
            throw new Exception("该命令操作类型不对，只能compress或uncompress !");
        }
    }

    public void listFiles(FileSystem fs, Path sourcePath) throws IOException {
        FileStatus[] files = fs.listStatus(sourcePath);

        for (FileStatus file : files) {
            if (file.isDirectory()) {
                listFiles(fs, file.getPath());
            } else {
                HdfsFile hdfsFile = new HdfsFile();
                hdfsFile.setPath(file.getPath());
                String path = file.getPath().toString();
                hdfsFile.setFileType(path.substring(path.lastIndexOf(".") + 1));
                hdfsFile.setFileName(file.getPath().getName());
                String parentPath = file.getPath().getParent().toString();
                hdfsFile.setFileDate(parentPath.substring(parentPath.lastIndexOf(File.separator) + 1));

                listFiles.add(hdfsFile);
            }
        }
    }

    public void compress(String codeClass, Configuration conf, String sourceDir, String targetDir, String sourceType, String targetType) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path sourcePath = new Path(sourceDir);

        listFiles(fs, sourcePath);

        Class clz = Class.forName(codeClass);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clz, conf);

        for (HdfsFile file : listFiles) {
            Path targetPath = new Path(String.format("%s/%s/%s.gz", targetDir, file.getFileDate(), file.getFileName()));
            compressFile(file.getPath(), targetPath, fs, codec, conf, file.getFileType(), targetType);
        }
    }

    private void compressFile(Path inPath, Path outPath, FileSystem fs, CompressionCodec codec, Configuration conf, String sourceType, String targetType) {
        if (inPath.getName().indexOf(sourceType) > 0) {
            FSDataInputStream input = null;
            CompressionOutputStream compressOut = null;
//            long fileLength = (new File(path.toUri()).length()) / 65536;
            try {
                input = fs.open(inPath);

                System.out.println(String.format("开始压缩: [%s] -> [%s]", inPath.toString(), outPath.toString()));
                // 回调函数显示进度， 这个进度条是 64K 打印一次， 没找到控制的地方
//                FSDataOutputStream output = fs.create(new Path(path.toString().replace(sourceType, targetType)), new Progressable() {
////                    int fileCount = 0;
//
//                    // 每一次回调都是输出 64K ， 可以使用这个打印百分数进度
//                    @Override
//                    public void progress() {
////                        fileCount++;
////                        System.out.println(fileCount / fileLength);
//                        System.out.print("*");
//                    }
//                });

//                FSDataOutputStream output = fs.create(new Path(path.toString().replace(sourceType, targetType)), true, 8096, new Progressable() {
//                    @Override
//                    public void progress() {
//                        System.out.print("*");
//                    }
//                });

                // 不打印进度
                FSDataOutputStream output = fs.create(outPath, true, 8192);
                compressOut = codec.createOutputStream(output);
                IOUtils.copyBytes(input, compressOut, 8192, true);
//                System.out.println("完成\n");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(input);
                IOUtils.closeStream(compressOut);
            }
        }
    }
}