import pandas as pd
import csv
from math import isnan
from datetime import date
import pickle

chunkcount=0

def avg(l):
	return sum(l)/len(l)

print("starting to read train")
chunksize = 10 ** 6
train = pd.read_csv("../Expedia_Dataset/train.csv",chunksize=chunksize,
	usecols=["is_booking","hotel_cluster",'user_location_country',"user_location_region","user_location_city","srch_destination_type_id","hotel_continent","hotel_country","hotel_market","srch_adults_cnt","srch_children_cnt","srch_rm_cnt","srch_ci","srch_co","user_id","srch_destination_id"])
print("read train.")
maxuid=0
max_stay= 370
max_user_id= 9999
max_user_location_country= 239
max_user_location_region =  1017
max_user_location_city =  56507
max_srch_destination_type_id= 9
max_srch_destination_id= 65068
max_hotel_continent= 6
max_hotel_country= 212
max_hotel_market= 2117
max_srch_adults_cnt= 9
max_srch_children_cnt= 9
max_srch_rm_cnt= 8
min_stay= 0
min_user_id= 0
min_user_location_country= 0
min_user_location_region =  0
min_user_location_city =  0
min_srch_destination_type_id= 1
min_srch_destination_id= 4
min_hotel_continent= 0
min_hotel_country= 0
min_hotel_market= 0
min_srch_adults_cnt= 0
min_srch_children_cnt= 0
min_srch_rm_cnt= 1

avg_stay= 3.3225658748560014
avg_user_id= 4944.773110518104
avg_user_location_country= 60.43104831065809
avg_user_location_region =  257.1908424107208
avg_user_location_city =  24729.94279714773
avg_srch_destination_type_id= 2.538046283739446
avg_srch_destination_id= 14185.990218294764
avg_hotel_continent= 3.376820216690727
avg_hotel_country= 92.84087376234014
avg_hotel_market= 552.2467357860507
avg_srch_adults_cnt= 1.9634775507823323
avg_srch_children_cnt= 0.28567012263586916
avg_srch_rm_cnt= 1.1229304285693453

ucount=0
max_min_user_location_country = max_user_location_country - min_user_location_country
max_min_user_location_region = max_user_location_region - min_user_location_region
max_min_user_location_city = max_user_location_city - min_user_location_city
max_min_srch_destination_type_id = max_srch_destination_type_id - min_srch_destination_type_id
max_min_srch_destination_id = max_srch_destination_id - min_srch_destination_id
max_min_hotel_continent = max_hotel_continent - min_hotel_continent
max_min_hotel_country = max_hotel_country - min_hotel_country
max_min_hotel_market = max_hotel_market - min_hotel_market
max_min_srch_adults_cnt = max_srch_adults_cnt - min_srch_adults_cnt
max_min_srch_children_cnt = max_srch_children_cnt - min_srch_children_cnt
max_min_srch_rm_cnt = max_srch_rm_cnt - min_srch_rm_cnt
max_min_stay = max_stay - min_stay
max_min_user_id = max_user_id - min_user_id
def abs(x):
	if x<0:
		return -x
	else:
		return x
vectors = []
outputs = []
for chunk in train:
	chunkcount+=1
	print("chunk",chunkcount,chunk.shape)
	for ind in chunk.index:
		uid = chunk['user_id'][ind]
		if uid>=10000:
			continue
		ci = chunk["srch_ci"][ind]
		co = chunk["srch_co"][ind]
		try:
			if isnan(float(ci)) or isnan(float(co)):
				pass
		except Exception as e:
			l = ci.split("-")
			d0 = date(int(l[0]),int(l[1]),int(l[2]))
			l = co.split("-")
			d1 = date(int(l[0]),int(l[1]),int(l[2]))
			delta = d1 - d0
			date_diff = abs(delta.days)
			ucount+=1
			user_location_countries = ((chunk["user_location_country"][ind]-avg_user_location_country)/(max_min_user_location_country))
			user_location_regions = ((chunk["user_location_region"][ind]-avg_user_location_region)/(max_min_user_location_region))
			user_location_cities = ((chunk["user_location_city"][ind]-avg_user_location_city)/(max_min_user_location_city))
			srch_destination_type_ids = ((chunk["srch_destination_type_id"][ind]-avg_srch_destination_type_id)/(max_min_srch_destination_type_id))
			hotel_continents = ((chunk["hotel_continent"][ind]-avg_hotel_continent)/(max_min_hotel_continent))
			hotel_countries = ((chunk["hotel_country"][ind]-avg_hotel_country)/(max_min_hotel_country))
			hotel_markets = ((chunk["hotel_market"][ind]-avg_hotel_market)/(max_min_hotel_market))
			srch_adults_cnts = ((chunk["srch_adults_cnt"][ind]-avg_srch_adults_cnt)/(max_min_srch_adults_cnt))
			srch_children_cnts = ((chunk["srch_children_cnt"][ind]-avg_srch_children_cnt)/(max_min_srch_children_cnt))
			srch_rm_cnts = ((chunk["srch_rm_cnt"][ind]-avg_srch_rm_cnt)/(max_min_srch_rm_cnt))
			stay = ((date_diff-avg_stay)/(max_min_stay))
			user_ids = ((chunk["user_id"][ind]-avg_user_id)/(max_min_user_id))
			srch_destination_ids = ((chunk["srch_destination_id"][ind]-avg_srch_destination_id)/(max_min_srch_destination_id))
			vector = [user_location_countries,user_location_regions,user_location_cities,srch_destination_type_ids,hotel_continents, hotel_countries,hotel_markets,srch_adults_cnts,srch_children_cnts,srch_rm_cnts,stay,user_ids,srch_destination_ids]
			output = [0,0]
			if chunk["is_booking"][ind]==1:
				output[0] = 1
				output[1] = chunk["hotel_cluster"][ind]
			else:
				output[0] = 0
				output[1] = chunk["hotel_cluster"][ind]
			vectors.append(vector)
			outputs.append(output)
	print("processed",chunkcount,"users:",ucount)
print(ucount)
print(len(outputs))
with open("feature/vectors.data","wb") as file:
	pickle.dump(vectors,file)
with open("feature/outputs.data","wb") as file:
	pickle.dump(outputs,file)
# print(nans)
