package com.gertent.hdfs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用FileSystem操作HDFS
 * @author wyf
 * @date 2018/1/9
*/
public class AppDemo2 {
    private static final String HDFS_PATH = "hdfs://localhost:9000";
    private static final String DIR_PATH = "/d100";
    private static final String FILE_PATH = "/d100/d1000";
    public static void main(String[] args) throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
        //创建文件夹
        makeDir(fileSystem);
        //上传文件
        //uploadData(fileSystem);
        //下载文件
//      downloadData(fileSystem);
        //删除文件
//      deleteData(fileSystem);
    }

    /**
     * 删除文件
     * @param fileSystem
     * @throws IOException
     */
    private static void deleteData(FileSystem fileSystem) throws IOException {
        fileSystem.delete(new Path(FILE_PATH), true);
    }

    /**
     * 下载文件
     * @param fileSystem
     * @throws IOException
     */
    private static void downloadData(FileSystem fileSystem) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));
        IOUtils.copyBytes(in, System.out, 1024, true);
    }

    /**
     * 创建文件夹
     * @param fileSystem
     * @throws IOException
     */
    private static void makeDir(FileSystem fileSystem) throws IOException {
        fileSystem.mkdirs(new Path(DIR_PATH));
    }

    /**
     * 上传文件
     * @param fileSystem
     * @throws IOException
     * @throws FileNotFoundException
     */
    private static void uploadData(FileSystem fileSystem) throws IOException,
            FileNotFoundException {
        FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH));
        InputStream in = new FileInputStream(new File("d:/log.txt"));
        IOUtils.copyBytes(in, out, 1024, true);
    }
}
