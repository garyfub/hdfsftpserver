

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class Test {

	public static String hdfsUri = "hdfs://10.0.0.84:9000/";
	public static String hdfsUser = "xtjc";
	
	public static void main(String[] args) throws IOException{
		DistributedFileSystem temp = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("HADOOP_USER_NAME", hdfsUser);
		DistributedFileSystem dfs = null;
		try {
			temp.initialize(new URI(hdfsUri), conf);
			dfs = temp;
		
			System.out.println("***&");
			
            Path path = new Path("/");

            iteratorShowFiles(dfs, path);
            
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
			System.out.println("ERROR|FtpFileSystemView|initialize");
		}finally {
			if (null != dfs) {
				dfs.close();
			}
		}
	}
	
	
	 /**
     * 
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFiles(FileSystem hdfs, Path path){
        try{
            if(hdfs == null || path == null){
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);

            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try{
                    if(files[i].isDirectory()){
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath());
                    }else if(files[i].isFile()){
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
