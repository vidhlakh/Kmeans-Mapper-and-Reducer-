import sys
import math
datafile=sys.argv[1]
#centfile=sys.argv[2]

datapoints = []
centroids = []
#Retrieving data points 
try:
	df= open(datafile,"r") 
	filecontents = df.read()
	for line in filecontents.strip().split("\n"):
		data= line.strip().split(",")
		x = float(data[0])
		y=float(data[1])
		coord = (x,y)
		datapoints.append(coord)
except Exception as e:
		print("Unexpected error:",e)
		raise 

	
# Retrieve centroids from file
try:
	cf= open("/user/vidhya/centroids","r") 
	centfilecontents = cf.read()
	#print("centfile",centfilecontents)
	for line in centfilecontents.strip().split("\n"):
		cen= line.strip().split("\t")
		clusid = cen[0]
		pts = (cen[1].split(","))
		Cx=float(pts[0])
		Cy=float(pts[1])
		cent = (Cx,Cy)
		centroids.append(cent)
except Exception as e:
		print("Unexpected error:",e)
		raise 

#print("centroids",centroids)	
#print("length",len(centroids))

result = {}
#computing euclidean distance

def dist(pt1,pt2):
	distvalue=math.sqrt((pt1[0]-pt2[0])**2+(pt1[1]-pt2[1])**2)
	return distvalue

for data in datapoints:
	#print("data",data)
	mindist=dist(data,centroids[0])
	label=0
	t=dist(data,centroids[1])
	if(mindist>t):
		mindist=t
		label =1
	result={label:data}
	#Printing the label and data
	for key,value in result.items():
		print(value[0],",",value[1],"\t",key)

		

