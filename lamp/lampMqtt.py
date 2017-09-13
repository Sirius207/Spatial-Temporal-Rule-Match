import paho.mqtt.client as mqtt
import time
import datetime
import math
import psycopg2
import json
from math import radians, cos, sin, asin, sqrt

from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC']
TIME_RATE = CONFIG['TIME_RATE']
DISTANCE = CONFIG['DISTANCE']
TIMELINE = CONFIG['TIMELINE']
POINT_COUNTS = CONFIG['POINT_COUNTS']

from location import LOCATION

from config import DBCONFIG
HOST = DBCONFIG['HOST']
DBNAME = DBCONFIG['DBNAME']
USER = DBCONFIG['USER']
PASSWORD = DBCONFIG['PASSWORD']

EventDict = dict()
DistanceDict = dict()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
		client.subscribe(TOPIC)

def on_publish(client, userdata, mid):
		print("mid: "+str(mid))

def distance(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	m = 6367 * c * 1000
	return m


def TimeDifference(TimeA,TimeB):
	tmpA = datetime.datetime.strptime(TimeA, "%Y-%m-%d").date()
	tmpB = datetime.datetime.strptime(TimeB, "%Y-%m-%d").date()
	DateDelta = ( tmpA - tmpB ).days
	if DateDelta < 0:
		DateDelta = (-1) * DateDelta
	return DateDelta
	

def CheckPointInMcc(p,Mcc):
	global DistanceDict
	for x in Mcc:
		if (x,p) not in DistanceDict:
			return False
	if len(Mcc) == 2:
		if DistanceDict[(Mcc[0],p)] > DistanceDict[(Mcc[0],Mcc[1])] or DistanceDict[(Mcc[1],p)] > DistanceDict[(Mcc[0],Mcc[1])] :
			return False
		if DistanceDict[(Mcc[0],p)]**2 + DistanceDict[(Mcc[1],p)]**2 > DistanceDict[(Mcc[0],Mcc[1])]**2:
			return False
		return True
	elif len(Mcc) == 3:
		# ABC vs P
		a = DistanceDict[(p,Mcc[0])] # PA
		b = DistanceDict[(p,Mcc[1])] # PB
		c = DistanceDict[(p,Mcc[2])] # PC
		Pmin = a
		Pmax = (b,c)
		brother = Mcc[0]
		Bmax = (DistanceDict[(Mcc[0],Mcc[1])],DistanceDict[(Mcc[0],Mcc[2])])
		Base = DistanceDict[(Mcc[1],Mcc[2])]
		
		if b < Pmin:
			Pmin = b
			Pmax = (a,c)
			brother = Mcc[1]
			Bmax = (DistanceDict[(Mcc[0],Mcc[1])],DistanceDict[(Mcc[1],Mcc[2])])
			Base = DistanceDict[(Mcc[0],Mcc[2])]
		if c < Pmin:
			Pmin = c
			Pmax = (a,b)
			brother = Mcc[2]
			Bmax = (DistanceDict[(Mcc[0],Mcc[2])],DistanceDict[(Mcc[1],Mcc[2])])
			Base = DistanceDict[(Mcc[0],Mcc[1])]

		if Pmax[0]**2 + Pmax[1]**2 <= Base**2:	# degree more than 90
			return True

		CosP = (Pmax[0]**2 + Pmax[1]**2 - Base**2) / (2*Pmax[0]*Pmax[1])
		CosB = (Bmax[0]**2 + Bmax[1]**2 - Base**2) / (2*Bmax[0]*Bmax[1])
		if CosP <= CosB :
			return True
		return False

'''
def GetNewMcc(p,OldMcc):
	global DistanceDict
	if len(Mcc) == 2:
		if DistanceDict[(Mcc[0],p)]**2 + DistanceDict[(Mcc[1],p)]**2 <= DistanceDict[(Mcc[0],Mcc[1])]**2:
			return OldMcc
		else:
'''

		
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	def TempSaveMcc(Key, NewSet):
		'''Remove Subset of MCC '''
		isNotSubset = True
		PopSet = set()
		for PrevSet in MccSet:
			if(  NewSet.issubset( set(PrevSet) ) ):
				isNotSubset = False
			if(  PrevSet.issubset( NewSet ) and len(PrevSet) != len(NewSet)):
				PopSet.add(PrevSet)

		for PrevSet in PopSet:
			MccSet.discard(PrevSet)
			del MccKey[PrevSet]
	
		if (isNotSubset):
			#print ('save new mcc')
			MccSet.add(frozenset(NewSet))
			MccKey[frozenset(NewSet)] = list(Key)

	def CalMccCenter(mcc):
		'''Calculate the center position of Mcc '''
		if len(mcc) == 2:
			center = list()
			center.append( (EventDict[mcc[0]]["Lon"] + EventDict[mcc[1]]["Lon"]) / 2 )
			center.append( (EventDict[mcc[0]]["Lat"] + EventDict[mcc[1]]["Lat"]) / 2 )
			#print (center)
			return center
		else:
			return CalMccCenterByThreePoints(mcc)

	def CalMccCenterByThreePoints(mcc):
		'''Calculate Mcc Center By Three Points'''
		center = list()
		ma = (EventDict[mcc[1]]["Lat"] - EventDict[mcc[0]]["Lat"]) / (EventDict[mcc[1]]["Lon"] - EventDict[mcc[0]]["Lon"])
		mb = (EventDict[mcc[2]]["Lat"] - EventDict[mcc[1]]["Lat"]) / (EventDict[mcc[2]]["Lon"] - EventDict[mcc[1]]["Lon"])
		centerLon = (ma*mb*(EventDict[mcc[0]]["Lat"] - EventDict[mcc[2]]["Lat"]) + mb*(EventDict[mcc[0]]["Lon"] + EventDict[mcc[1]]["Lon"]) \
								- ma*(EventDict[mcc[1]]["Lon"] + EventDict[mcc[2]]["Lon"]) ) / (2*(mb-ma))

		centerLat = -(1/ma) * (centerLon - (EventDict[mcc[0]]["Lon"] + EventDict[mcc[1]]["Lon"])/2) \
								+ (EventDict[mcc[1]]["Lat"] + EventDict[mcc[0]]["Lat"])/2

		center.append(centerLon)
		center.append(centerLat)
		#print str(center[1]) + ',' + str(center[0])
		return center


	if msg.topic=='ncku/netdb/test':
		print (str(msg.payload))
	elif msg.topic==TOPIC:
		# Rule
		D = DISTANCE
		N = TIME_RATE
		print (str(msg.payload))
		# Initial
		global EventDic
		global DistanceDict
		RedIndex = set()
		BlackIndex = set()
		Neighber = set()

		# Mcc Elements
		MccSet = set()
		# Mcc Keys
		MccKey = dict()
		LampData = json.loads(msg.payload)
		lampId = str(LampData['id'])
		Eid = str(time.time()).replace(".","") + '-' + lampId
		Lon = float(LOCATION[lampId]['Lon'])
		Lat = float(LOCATION[lampId]['Lat'])
		Time = str(datetime.date.today())

		# Generate Red, Black and Neighber
		PopList = list()
		for x in EventDict:
			Rflag = False
			Bflag = False
			Tdiff = TimeDifference(Time,EventDict[x]["Time"]) #######################
			if Tdiff <= 1 :
				#RedIndex.add(x)
				Rflag = True
			elif Tdiff <= TIMELINE and Tdiff > 1 :
				#BlackIndex.add(x)
				Bflag = True
			else :
				#EventDict.pop(x,None)
				#print "KEVIN OLD POINT : "+str(x)
				PopList.append(x)
				continue

			
			DistanceTmp = distance(Lon,Lat,EventDict[x]["Lon"],EventDict[x]["Lat"])
			#print "KEVIN DISTANCE : "+str(DistanceTmp)
			if DistanceTmp <= (2*D) :
				if Rflag:
					RedIndex.add(x)
				if Bflag:
					BlackIndex.add(x)
				Neighber.add(x)
				DistanceDict[(x,Eid)] = DistanceTmp	# from small to large
				DistanceDict[(Eid,x)] = DistanceTmp	# from large to small

		Dpop = set()
		for x in PopList:
			del EventDict[x]
			for y in DistanceDict:
				if x in y:
					Dpop.add(y)
		for x in Dpop:
			del DistanceDict[x]

		# Get Mcc about Eid (2-point and 3-point)
		Mcc = dict()

		for x in Neighber:
			Mcc[(x,Eid)] = set([x,Eid])	# 2-point Mcc
			for y in EventDict[x]["Neighber"]: # Eid > x > y
				if y not in Neighber:
					continue
				a = DistanceDict[(x,Eid)]
				b = DistanceDict[(y,Eid)]
				c = DistanceDict[(y,x)]
				Emax = a
				Emin = (b,c)
				diameter = (x,Eid)
				if b > Emax:
					Emax = b
					Emin = (a,c)
					diameter = (y,Eid)
				if c > Emax:
					Emax = c
					Emin = (a,c)
					diameter = (y,x)

				if Emin[0]**2 + Emin[1]**2 <= Emax**2:
					Mcc[diameter] = set([y,x,Eid])	# 3 point in 2-point Mcc
				else:
					S = (a+b+c)/2
					R = a*b*c/(4*math.sqrt(S*(S-a)*(S-b)*(S-c)))
					if R > D:
						continue
					Mcc[(y,x,Eid)] = set([y,x,Eid])	# 3-point Mcc

		#print ("KEVIN Eid : "+str(Eid))
		#print "KEVIN DistanceDict : "
		#print DistanceDict
		#print "KEVIN NEIGHBER : "
		#print Neighber
		
		for p in Neighber:
			for mcc in Mcc:
				if CheckPointInMcc(p,mcc):
					Mcc[mcc].add(p)
		
		# Get Score of Mcc about Eid
		for mcc in Mcc:
			Pnumber = len(Mcc[mcc])
			RedNumber = len(Mcc[mcc]&RedIndex)
			BlackNumber = Pnumber - RedNumber
			if BlackNumber == 0 :
				# print mcc
				# print Mcc[mcc]
				TempSaveMcc(mcc, Mcc[mcc])


				continue
			if RedNumber / BlackNumber >= N:
				# print mcc
				# print Mcc[mcc]
				TempSaveMcc(mcc, Mcc[mcc])

		# Get Score of Mcc about Eid's Neighber
		MccPop = set()
		for x in Neighber:
			for mcc in EventDict[x]["Mcc"]:
				if mcc[0] not in RedIndex and mcc[0] not in BlackIndex :
					#EventDict[x]["Mcc"].pop(mcc,None)
					MccPop.add((x,mcc))
					continue
				if CheckPointInMcc(Eid,mcc):
					EventDict[x]["Mcc"][mcc].add(Eid)
				else:
					continue
				Pnumber = len(EventDict[x]["Mcc"][mcc])
				RedNumber = len(EventDict[x]["Mcc"][mcc]&RedIndex)
				BlackNumber = Pnumber - RedNumber
				if BlackNumber == 0:
					# print mcc
					# print EventDict[x]["Mcc"][mcc]
					TempSaveMcc(mcc, EventDict[x]["Mcc"][mcc])
					continue
				if RedNumber / BlackNumber >= N:
					# print mcc
					# print EventDict[x]["Mcc"][mcc]
					TempSaveMcc(mcc, EventDict[x]["Mcc"][mcc])

		for x in MccPop:
			EventDict[x[0]]["Mcc"].pop(x[1],None)


		# Add To EventDict

		EventDict[Eid] = dict()
		EventDict[Eid]["Lon"] = Lon
		EventDict[Eid]["Lat"] = Lat
		EventDict[Eid]["Time"] = Time
		EventDict[Eid]["Neighber"] = Neighber
		EventDict[Eid]["Mcc"] = Mcc
		cursor.execute("INSERT INTO lamps (id, created_at, counts) \
		VALUES(%s, %s, %s);", (LampData['id'], datetime.datetime.now(), int(LampData['cnt'])))
		conn.commit()

		#print (Eid)
		for mcc in MccSet:
			if (len(mcc) > POINT_COUNTS):
				#print ('Save to DB: ')
				#print (MccKey[mcc])
				#print (list(mcc))
				# calculate the center of mcc
				center = CalMccCenter(MccKey[mcc])
				# save mcc to db
				cursor.execute("INSERT INTO lamp_mccs (mcc_keys, mcc_lists, created_at, distance, timeline, point_counts, center) \
	VALUES(%s, %s, %s, %s, %s, %s, %s);", (MccKey[mcc], list(mcc) , Time, DISTANCE, TIMELINE, POINT_COUNTS, list(center)) )
				conn.commit()




conn_string = "host="+HOST+" dbname="+DBNAME+" user="+USER+" password="+PASSWORD
print ("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print ("Connected!\n")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_ADDRESS, 1883, 60)
client.loop_forever()
