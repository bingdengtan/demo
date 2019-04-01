import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs._;
import java.io.PrintWriter;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import org.apache.hadoop.io.IOUtils;
import java.text.SimpleDateFormat

object Main extends App {			
	val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
	val conf = new Configuration();
	val hdfsUrl = "hdfs://172.19.37.11:8020"
	val realUrl = "/tmp/spark/mySample.txt"
	val parentUrl = "/tmp/spark"
	
	conf.set("fs.defaultFS", "hdfs://172.19.37.11:8020")
	conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
	conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
	
	while(true){
		val fs = FileSystem.get(conf)
		
		val ts = System.currentTimeMillis()
		val str = df.format(ts) + "\r\n"
		
		println("try to append timestamp: " + str)
		
		if(!fs.exists(new Path(parentUrl))){
			println("Doesn't exit parent folder: " + parentUrl)
			fs.mkdirs(new Path(parentUrl))
		}	
		if(!fs.exists(new Path(realUrl))){
			println("Doesn't exit file: " + realUrl)
			val os = fs.create(new Path(realUrl))
			os.write(str.getBytes("UTF-8"))
			os.close();
		}else{
			val outputStream = fs.append(new Path(realUrl))
			val writer = new PrintWriter(outputStream)
			writer.append(str);
			writer.close();
		}
		fs.close();
		Thread.sleep(1000*60)
	}
}
