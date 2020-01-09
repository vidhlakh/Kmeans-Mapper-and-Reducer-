import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
									 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KMeans
{
   
  public static class KMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

  private final static IntWritable clustlabel = new IntWritable();
  private Text record = new Text();
 
  private double [][]  centroids = new double[2][2];
  private double[][] data;
  int rows =5;
  protected void setup(Context context) throws IOException, InterruptedException
             
                 {

	   // Get parameters 
	
	   Configuration conf = context.getConfiguration();
	   String fileName = conf.get("centroids");
	  
	   Path path =new Path(fileName);
	   FileSystem f = FileSystem.get(conf);
	   
	   //read centroids from file
	   try
	   { 
			BufferedReader buf = new BufferedReader(new InputStreamReader(f.open(path)));
			centroids = loadcentroid(buf);
		   
   	}
	   catch(Exception e)
	   {
		   System.err.print("Error in reducer"+e); 
	   }
  
															   
															  
   
     }
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
   
	  
	     String [] tempstore = new String[2];
	     StringTokenizer tokens = new StringTokenizer(value.toString());
        
       	data = new double[rows][];
	      for (int i=0; i<rows; i++)
	    	  data[i] = new double[2];
	      //Fetching data from data.csv and writing into record in string representation 
		while (tokens.hasMoreTokens()) {
			
			tempstore =tokens.nextToken().split(",");
			double[] dbltempstore = new double[tempstore.length];
			for (int k = 0; k < dbltempstore.length; k++) {
			    dbltempstore[k] = Double.parseDouble(tempstore[k]);
			}
			data[0]=dbltempstore;
			
			int clusterlab = closest(data[0],centroids);
			//set key map output
			clustlabel.set(clusterlab);
			StringBuffer stbuffer = new StringBuffer();
			for(int j =0;j<data[0].length;j++){
				if(j==0){
		     String str1=Double.toString(data[0][j]) ;
		     stbuffer.append(str1+",");
				}
				else{
			String str2 = Double.toString(data[0][j]);
			stbuffer.append(str2);
				}
		     
			}
			String rec =stbuffer.toString();
			
			record.set(rec);
			 
	     context.write(clustlabel, record);
	  
		}
  }
  // Finding the euclidean dist
  public static double dist(double [] v1, double [] v2){
    double sum=0;
    for (int i=0; i<2; i++){
      double d = v1[i]-v2[i];
      sum += d*d;
    }
    return Math.sqrt(sum);
  }
		// Finding the closest dist between centroid and data values
	    public static int closest(double [] data,double [][] cent)
	    {
	      double firstclust = dist(data, cent[0]);
	      int label =0;
	     double secclust = dist(data, cent[1]);
	        if (firstclust>secclust){
	          label = 1;
	        }
	      return label;
	    }
	   
	   

}

													 
public static class KReducer extends Reducer<IntWritable,Text,IntWritable, Text> 
{
	
 
  private Text rcentroid = new Text();
  public static String record;
  public static double[][] redcentroids = new double[2][2];
  
  public void reduce(IntWritable keys, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {
	  
	   int rows=5;
	  int label = keys.get(); 
							
						  
	  double[][] data = new double[rows][];
	  
	  for(int i =0;i<rows;i++){
	 	data[i]= new double[2];
	  }
	  
	  //Get data values
	  String [] tempdata = new String[5];
	  int count =0;
	 
	 
		 try
		 {
	  for (Text val : values) 
	  {
			
			tempdata =val.toString().split(",");
			double[] nums = new double[tempdata.length];
			for (int k = 0; k < nums.length; k++) {
			    nums[k] = Double.parseDouble(tempdata[k]);
			}
			data[count]=nums;
			count++;
			 
	  }
		 }
		 catch(Exception ar)
		 {
			 System.err.print("Err"+ar); 
		 }
		 

 
												   
  
		 try {
			 
		 redcentroids= updateCentroids(data,count,label);
		
			StringBuffer stbuffer = new StringBuffer();
			for(int j =0;j<redcentroids[label].length;j++){
				if(j==0)
				{
		     String a=Double.toString(redcentroids[label][j]) ;
		     stbuffer.append(a+",");
				}
				else
				{
			String b = Double.toString(redcentroids[label][j]);
			stbuffer.append(b);
				}
		     
			}
		    record =stbuffer.toString();   
			
	  rcentroid.set(record);
		
	  context.write(keys, rcentroid); 
		 }
		 catch(Exception ar)
		 {
			 System.err.print("Error in reducer"+ar); 
		 }
  }
  public static double [][] updateCentroids(double[][]data,int count, int label){
	    // initialize centroids and set to 0
	  
	   double [][] newCentroid = new double [count][2];
	   
	 

	    // intialize
	   for (int i=0; i<count; i++)
	   {
										
	      for (int j=0; j<2; j++)
	           newCentroid[i][j] =0;
	    }
	    //sum all data in the same cluster
	    for (int i=0; i<count; i++){
	        for (int j=0; j<2; j++){
	        	
	          newCentroid[label][j] += data[i][j]; // update that centroid by adding the member data record
	         // System.out.println("new cent"+newCentroid[label][j]+"data"+data[i][j]);
	        }
	        
	      }
	      // find mean
	      for (int i=0; i< 2; i++){
	        for (int j=0; j<2; j++){
	        	
	          newCentroid[i][j]/= count;
	         
	        }
	      }
	      
	      
	     
	      return newCentroid;
}
 
}

public static void main(String[] args) throws Exception {
	 String fileName;
	 double[][] previouscentroid=new double [2][2];
	 double[][] currentcentroid=new double [2][2];
  
  Configuration conf = new Configuration();
  
 String[] remArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
System.out.println("rem args"+remArgs[0]+remArgs[1]+remArgs[2]);	

									  
  //check for 3 arguments
  if(remArgs.length!=3)
  {
	 System.err.println("No correct input / Output arguments: Enter Input: data,centroids Output: dir");
	 System.exit(2);
  }
//oth argument is data file 
//1st Argument  is centroid file
//2nd argument is output dir
   conf.set("centroids", remArgs[1]);	
  int niter=10;
  int round=0;
  while(true)
  {
	  //create new job instance
  Job job = Job.getInstance(conf, "KMeans");
  	  // set input and output path
  Path datainputPath = new Path(remArgs[0]);
  Path outputPath = new Path(remArgs[2]);
  
  job.setJarByClass(KMeans.class);
  job.setMapperClass(KMapper.class);
  job.setReducerClass(KReducer.class);
  job.setOutputKeyClass(IntWritable.class);
  job.setOutputValueClass(Text.class);
  FileInputFormat.addInputPath(job, datainputPath);
  
  FileOutputFormat.setOutputPath(job, outputPath);
  	

  //replace centroid file with the output file 
 
  //Fetch configuration parameter and loading centroid file
  fileName = conf.get("centroids");
  Path path =new Path(fileName);
  FileSystem f = FileSystem.get(conf);
  BufferedReader bufread = new BufferedReader(new InputStreamReader(f.open(path)));
  previouscentroid =loadcentroid(bufread);
  
   /*while(round!=0)
  {
  try
  {
  File sourcefile = new File(remArgs[1]+"part-r-00000");
  File targetfile = new File("centroids");
  FileUtil.replaceFile(sourcefile, targetfile);
 
			
	  System.out.println("success");
	  BufferedReader bufread2 = new BufferedReader(new InputStreamReader(f.open(path)));
	  currentcentroid = loadcentroid(bufread2);
  }
  catch(Exception e)
  {
	  System.out.println("failed "+e);
  }
 
  }
  if (round ==0)
  {
 	 currentcentroid = previouscentroid;
	 	 
												   
  }*/
  
  
  try
  {
	 
										 
 // FileUtil.replaceFile(sourcefile, targetfile);
 	  System.out.println("success");
 	  	if(round==0) {
 	  	  BufferedReader bufread3 = new BufferedReader(new InputStreamReader(f.open(path)));
 	 	  currentcentroid = loadcentroid(bufread3);
 	  	}else
 	  	{
		  Path outputfilePath = new Path(remArgs[2]+"part-r-00000");
		  BufferedReader bufreadoutput = new BufferedReader(new InputStreamReader(f.open(outputfilePath)));
		  File file = new File(fileName);
		  FileWriter fw = new FileWriter(file);
	  BufferedWriter bw = new BufferedWriter(fw);
	  String result;
	  while((result = bufreadoutput.readLine())!=null)
	   {
		    System.out.println("result"+result);
		    bw.write(result);
		    bw.write("\n");
		   
	   }
	  bw.close();
	  BufferedReader bufread3 = new BufferedReader(new InputStreamReader(f.open(path)));
	  currentcentroid = loadcentroid(bufread3);
 	  	}  
	  
  }
catch(Exception e)
  {
	  System.out.println("Failed :: "+e);
  }
 
//To automatically delete output file for each execution
 outputPath.getFileSystem(conf).delete(outputPath,true);
  job.waitForCompletion(true);
 round++;
 
//converge if previous and current centroids are equal
if ((niter >0 && round >=niter) || converge(previouscentroid, currentcentroid))
{
	System.out.println("Clustering converges at round " + round);
    break;
  
}
 
 
}
 
//System.exit(0);
}
public static double[][] loadcentroid (BufferedReader bufread)
{
	
	double[][] centroid=new double [2][2];
	String[][]  part = new String[2][2];
	 
	   String result;
	   String[] fr1;
	   String[] fr2;
	   String[][] frecords =new String[2][2]; 
	   try
	   {
	  for(int i=0;i<2;i++)
	  {
		 if((result = bufread.readLine())!=null);
		   {
			    part[i] = result.split("\t");
			    
			   
		   }
	  }
	   
		   fr1= part[0][1].split(",");
		   fr2= part[1][1].split(",");
	
		for(int i=0;i<2;i++)
		{
			   for(int j=0;j<2;j++)
			   {
				  if(i==0)
				  {
					  frecords[i][j]=fr1[j];
				  }
				  if(i==1)
				  {
					  frecords[i][j]=fr2[j];
				  }
				   
			   }
		}
	for(int i=0;i<2;i++)
	{
		   for(int j=0;j<2;j++)
		   {
			 
				   centroid[i][j] = Double.parseDouble(frecords[i][j]);
				  
			      
			   
		   }
	}
	   }
	   catch(IOException io)
	   {
		   System.out.println("Error in load centroid file: IO Exception"+io );
	   }
	   catch(NullPointerException n)
	   {
														  
	 
				 
 
		   System.out.println("Error in load centroid file"+n );
	   }
	   catch(Exception e)
	   {
		   System.out.println("Error in load centroid file"+e );
	   }
	return centroid;
}
private static boolean converge(double [][] c1, double [][] c2)
{
    // c1 and c2 are two sets of centroids 
    double threshold=0.001;
    double maxv = 0;
    
    for (int i=0; i< 2; i++){
        double d= KMapper.dist(c1[i], c2[i]);
        if (maxv<d)
            maxv = d;
    } 

    if (maxv <threshold)
      return true;
    else
      return false;
    
  }
 
}
