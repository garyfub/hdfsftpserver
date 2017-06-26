package com.gen.HdfsFtpServer.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsFtpFileSystemView implements FileSystemView {
	public static String hdfsUri = "hdfs://10.0.0.84:9000/";
	public static String hdfsUser = "xtjc";

	// current user work directory
	public String current = "/";

	public DistributedFileSystem dfs = null;

	public HdfsFtpFileSystemView(User user) {
		// System.out.println("PROCESS|FtpFileSystemView|initialize");

		if (dfs == null) {
			DistributedFileSystem temp = new DistributedFileSystem();
			Configuration conf = new Configuration();
			conf.set("HADOOP_USER_NAME", hdfsUser);
			try {
				temp.initialize(new URI(hdfsUri), conf);
				dfs = temp;
			} catch (IOException | URISyntaxException e) {
				e.printStackTrace();
				System.out.println("ERROR|FtpFileSystemView|initialize");
			}
		}
	}

	@Override
	public void dispose() {
		// System.out.println("PROCESS|FtpFileSystemView|dispose");

		if (dfs == null)
			return;

		try {
			dfs.close();
		} catch (IOException e) {
			System.out.println("ERROR|FtpFileSystemView|dispose");
		}
	}

	@Override
	public boolean changeWorkingDirectory(String path) throws FtpException {
		// System.out.println("PROCESS|FtpFileSystemView|changeWorkingDirectory
		// path: " + path + " current: " + current);
		if (path.startsWith("/")) {
			current = path;
		} else if (path.equals("..")) {
			current = current.substring(0, current.lastIndexOf("/"));
			if (current.equals(""))
				current = "/";
		} else if (current.endsWith("/")) {
			current = current + path;
		} else {
			current = current + "/" + path;
		}
		// System.out.println("PROCESS|FtpFileSystemView|changeWorkingDirectory
		// path: " + path + " current: " + current);
		return true;
	}

	@Override
	public FtpFile getFile(String file) throws FtpException {
		// System.out.println("PROCESS|FtpFileSystemView|getFile file: " +
		// file);

		String path = "";

		if (file.startsWith("/")) {
			// move command need this.
			path = file;
		} else if (file.equals("./")) {
			path = current;
		} else if (current.equals("/")) {
			path = current + file;
		} else {
			path = current + "/" + file;
		}

		return new HdfsFtpFile(path, this);
	}

	@Override
	public FtpFile getHomeDirectory() throws FtpException {
		// System.out.println("PROCESS|FtpFileSystemView|getHomeDirectory");
		return new HdfsFtpFile("/", this);
	}

	@Override
	public FtpFile getWorkingDirectory() throws FtpException {
		// System.out.println("PROCESS|FtpFileSystemView|getWorkingDirectory: "
		// + current);
		return new HdfsFtpFile(current, this);
	}

	@Override
	public boolean isRandomAccessible() throws FtpException {
		return true;
	}
	
	public static void main(String[] args) throws IOException{
		DistributedFileSystem temp = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("HADOOP_USER_NAME", hdfsUser);
		DistributedFileSystem dfs = null;
		try {
			temp.initialize(new URI(hdfsUri), conf);
			dfs = temp;
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
			System.out.println("ERROR|FtpFileSystemView|initialize");
		}finally {
			if (null != dfs) {
				dfs.close();
			}
		}
	}

}
