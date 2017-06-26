HdfsFtpServer

------------------------------------------------
0.2.0 LI Gen  201706081508

之前查看HDFS上的文件一直使用HDFS Explorer(http://bigdata.red-gate.com),
由于这个软件基于Web的接口.所以在使用的时候需要在客户机的hosts里配置所有节点,非常麻烦.

后来发现了hdfs-over-ftp-master(https://sites.google.com/a/iponweb.net/hadoop/Home/hdfs-over-ftp)
测试过程中发现里面的Apache FtpServer版本过低1.0.0-M3.
而最新的Apache FtpServer已经为1.1.1.
1.0.0-M3 -> 1.1.1的相关改动很大.于是就根据hdfs-over-ftp-master的思路重新写本软件HdfsFtpServer.


*JRE 1.8
*可运行版本为/bin目录下的内容
*当前支持的HDFS版本为2.7.2.其他版本请修改pom.xml相关版本并重新打包.
*目前不支持HDFS的权限管理.请在Hadoop的配置中关闭HDFS的权限管理.
*默认的账号密码是admin/admin  可以在user.properties中修改
*目前客户端使用测试了FileZilla.


*软件还在应用测试中.会不停的优化改进.还在不断完善中.
*如果有其他功能需求可以自行修改，或者联系我共同完善. li.gen@live.com








