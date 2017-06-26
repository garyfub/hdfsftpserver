package com.gen.HdfsFtpServer.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.hadoop.fs.Path;

public class HdfsFtpFile implements FtpFile
{
	public String path;
	
	public HdfsFtpFileSystemView view = null;
	
	public HdfsFtpFile(String path, HdfsFtpFileSystemView view)
	{
		//System.out.println("PROCESS|FtpHdfsFile|initialize");
		this.path = path;
		this.view = view;
	}

	
	@Override
	public boolean doesExist() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|doesExist path :" + path);
		try 
		{
			return view.dfs.exists(new Path(path));
		}
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|doesExist");
		}
		return false;
	}

	@Override
	public String getAbsolutePath() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|getAbsolutePath path :" + path);
		return path;
	}

	@Override
	public String getGroupName() 
	{
		//unknown used
		System.out.println("PROCESS|FtpHdfsFile|getGroupName path :" + path);
		return null;
	}
	
	@Override
	public int getLinkCount() 
	{
		//unknown used
		System.out.println("PROCESS|FtpHdfsFile|getLinkCount path :" + path);
		return 0;
	}

	@Override
	public String getName() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|getName path :" + path);
		
		try 
		{
			return view.dfs.getFileStatus(new Path(path)).getPath().getName();
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|getName");
		}	
		return "undefined";
	}

	@Override
	public String getOwnerName() 
	{
		//unknown used
		System.out.println("PROCESS|FtpHdfsFile|getOwnerName path :" + path);
		return null;
	}

	@Override
	public Object getPhysicalFile() 
	{
		//unknown used
		System.out.println("PROCESS|FtpHdfsFile|getPhysicalFile path :" + path);
		return null;
	}

	@Override
	public long getSize() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|getSize path :" + path);
		try 
		{
			return view.dfs.getFileStatus(new Path(path)).getLen();
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|getSize");
		}
		return 0;
	}

	@Override
	public boolean isDirectory() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isDirectory path :" + path);
		try 
		{
			return view.dfs.isDirectory(new Path(path));
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|isDirectory");
		}
		return false;
	}

	@Override
	public boolean isFile() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isFile path :" + path);
		try 
		{
			return view.dfs.isFile(new Path(path));
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|isFile");
		}
		return false;
	}
	
	@Override
	public long getLastModified() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|getLastModified path :" + path);
		try 
		{
			return view.dfs.getFileStatus(new Path(path)).getModificationTime();
		}
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|getLastModified");
		}
		return 0;
	}
	
	@Override
	public boolean setLastModified(long arg0) 
	{
		//unknown used
		System.out.println("PROCESS|FtpHdfsFile|setLastModified path :" + path);
		return false;
	}

	@Override
	public boolean isHidden() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isHidden :" + path);
		//HDFS File does not has hidden properties
		return false;
	}

	@Override
	public boolean isReadable() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isReadable() :" + path);
		return true;
	}

	@Override
	public boolean isRemovable() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isRemoveable() :" + path);
		return true;
	}

	@Override
	public boolean isWritable() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|isWritable() :" + path);
		return true;
	}

	@Override
	public List<FtpFile> listFiles() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|listFiles() :" + path);
		try 
		{
			return Arrays.asList(view.dfs.listStatus(new Path(path))).stream().map(v->new HdfsFtpFile(v.getPath().toString(), view)).collect(Collectors.toList());
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|listFiles");
		}
		return new ArrayList<FtpFile>();
	
	}

	@Override
	public boolean mkdir() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|mkdir path :" + path);
		try 
		{
			view.dfs.mkdirs(new Path(path));
			return true;
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|mkdir");
		}
		return false;
	}

	@Override
	public boolean move(FtpFile file) 
	{
		//System.out.println("PROCESS|FtpHdfsFile|move path from :" + path + " to " + file.getAbsolutePath());
		try 
		{
			return view.dfs.rename(new Path(path), new Path(file.getAbsolutePath()));
		} 
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|move");
		}
		return false;
	}

	@Override
	public boolean delete() 
	{
		//System.out.println("PROCESS|FtpHdfsFile|delete path :" + path);
		try 
		{
			return view.dfs.delete(new Path(path), true);
		}
		catch (IllegalArgumentException | IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|delete");
		}
		return false;
	}
	
	
	@Override
	public InputStream createInputStream(long arg0) throws IOException 
	{
		//System.out.println("PROCESS|FtpHdfsFile|createInputStream");
		try 
		{
			return view.dfs.open(new Path(path));
		} 
		catch (IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|createInputStream");
		}
		return null;
	}

	@Override
	public OutputStream createOutputStream(long arg0) throws IOException 
	{
		//System.out.println("PROCESS|FtpHdfsFile|createOutputStream");
		try 
		{
			view.dfs.createNewFile(new Path(path));
			return view.dfs.create(new Path(path));
		} 
		catch (IOException e) 
		{
			System.out.println("ERROR|FtpHdfsFile|createOutputStream");
		}
		return null;
	}


}
