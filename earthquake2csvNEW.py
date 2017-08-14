#===================================================================================
#This scrip file to read json file for Earthquake project Version2
#Editor: Nguyen Van Quan
#Date edit: 2016 Dec 27
#====================================================================================

#=================
# Libraries 
#=================
import re, os
import json
import csv
import time


file_in='stream_earthquake8_ngay21thag9.json'
file_out = os.path.splitext(file_in)[0]
interval_='60'
# 2 output in scv format
out1 = open('FromJsontoRaw%s_korea_kor.csv'%(file_out), 'w')
#out_time1 = open('FromJsontoRawKODUNG%sinterval%s.csv'%(file_out,interval_), 'w')

# create the csv writer object
#csv_time1 = csv.writer(out_time1)
#csv_time1.writerow(['Unix epoch','date','occurences%s'%interval_])

attribute=['id','created_at','timestamp_ms','user','id_str','lang','geo','text','json']
#csv.writerow(attribute)


t0=0;
inter_count=0;
m_count=1;

keyword_list=['korea','kor']

print ("Read Json of Tweets -----> CSV file")


with open(file_in, 'r') as f:
    	tweet_count = 0
	with out1 as f1:
		csv = csv.writer(f1, quoting=csv.QUOTE_ALL)
		csv.writerow(attribute)
	 	
		for line in f:
			#print '**************'
			if 'created_at' in line and '"lang":"en"' in line and any(keyword in line.lower() for keyword in keyword_list):  #and 'korea' in line.lower() #and '"lang":"en"' in line 
				tweet = json.loads(line) # load it as Python dict
				#print(line) # pretty-print
				tweet_count += 1
				row = (
			    	tweet['id'],                    # tweet_id
			    	tweet['created_at'],            # tweet_time
		 		tweet['timestamp_ms'],		# time Unix
			    	tweet['user']['screen_name'],   # tweet_author
			    	tweet['user']['id_str'],        # tweet_authod_id
			   	tweet['lang'],                  # tweet_language
			    	tweet['geo'],                   # tweet_geo
			    	tweet['text']                   # tweet_text
		       		)
		
				values = [(value.encode('utf8') if hasattr(value, 'encode') else value) for value in row]
				nl = [s.encode('utf8') for s in tweet]
				values.append(line)
				#if value[6] is None :
				#	print 'SAAAAAAAAAAAAAA'
				#else:
				#	print values[6]

				timestamp_ms=long(values[2])
				if t0==0:
					t0=timestamp_ms
					Acc_Tweets=0;
				if timestamp_ms<t0+float(interval_)*1000*m_count:
					inter_count+=1
				else:
					#csv_time1.writerow([t0+float(interval_)*1000*(m_count-1),time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime((t0+float(interval_)*1000*(m_count-1))/1000.)), inter_count])
					Acc_Tweets+=1;
					inter_count=1
					m_count+=1;
				#if 1493879043000 <= timestamp_ms <= 1494028800279:  #chon khoang thoi gian de de tinh toan nhieu
			 	csv.writerow(values)
				#with open(filename_geojson, mode='a') as ffff:
       					#ffff.write(json.dumps(tweet))
					#ffff.write('\n')
					#print (tweet)
        	

print "# Tweets Imported:", tweet_count
#print "# Tweets Accummulated:", Acc_Tweets

out1.close()
#out_time1.close()

