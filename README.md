# Kmeans-Mapper-and-Reducer-

Instructions to compile and Execute Java File
---------------------------------------------
1. Compile the KMeans.java file
sudo javac -classpath ${HADOOP_CLASSPATH} -d classes KMeans.java 
2. Create jar file from the classes.
 sudo jar -cvf KMeans.jar -C classes/ .
3. Create HDFS directory
hadoop jar KMeans.jar KMeans /user/vidhya/input /user/vidhya/output/
vidhlakh@ubuntu:/usr/local/KMeans$ hadoop fs -mkdir /user
hdvidhya@ubuntu:/usr/local/KMeans$ hadoop fs -mkdir /user/vidhya

4. Put Local file (data and centroids) into HDFS
vidhlakh@ubuntu:/usr/local/KMeans$ hadoop fs -put '/usr/local/KMeans/centroids' /user/vidhya
vidhlakh@ubuntu:/usr/local/KMeans$ hadoop fs -mkdir /user/vidhya/input
vidhlakh@ubuntu:/usr/local/KMeans$ hadoop fs -put '/usr/local/KMeans/data' /user/vidhya

5.Run the command 
vidhlakh@ubuntu:/usr/local/KMeans$hadoop jar KMeans.jar KMeans /user/vidhya /user/vidhya/centroids /user/vidhya/output/
Note: Order of input arguments is data file and centroid file

6.Output file 
bin/hadoop fs -cat /user/vidhya/output/part-00000

Instructions to compile and Execute Python File
------------------------------------------------
1. Copy data.hdfs input file to HDFS 
hadoop fs -copyFromLocal /usr/local/KMeans/data.hdfs /user/vidhya/python/input/data.hdfs 

2. Place the centroid file in the specific location "/user/vidhya/centroids" 
hadoop fs -copyFromLocal /usr/local/KMeans/centroids /user/vidhya/centroids
Note: centroid file path is set in python code

3. Execute the hadoop streaming mapper program (kmeansmapper.py) using the following command:
/usr/local/hadoop1/hadoop-3.2.0/bin/hadoop jar /usr/local/hadoop1/hadoop-3.2.0/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -input /user/vidhya/python/input/data.hdfs -output /user/vidhya/python/output1/ -mapper '/usr/local/python/KMeansmapper.py' 

4. Output file 
bin/hadoop fs -cat /user/vidhya/python/output1/part-00000

