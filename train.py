import pandas as pd
import csv
from math import isnan
from datetime import date
import pickle

print("starting to read train")
chunksize = 10 ** 6
train = pd.read_csv("../Expedia_Dataset/train.csv",chunksize=chunksize,usecols=["is_booking","hotel_cluster",'user_location_country',"user_location_region","user_location_city","srch_destination_type_id","hotel_continent","hotel_country","hotel_market","srch_adults_cnt","srch_children_cnt","srch_rm_cnt","srch_ci","srch_co"])
print("read train.")

checkin=[]
checkout=[]
max_day=0

date_diffs=[]
max_dist=0

is_bookings=[]

user_location_countries = []
max_user_loc_ctry=0

user_location_regions = []
max_user_loc_rgn = 0

user_location_cities = []
max_user_loc_cty = 0

srch_destination_type_ids = []
max_desttype_id = 0

hotel_continents = []
max_hotel_cnts = 0

hotel_countries = []
max_hotel_cntr = 0

hotel_markets = []
max_hotel_mrkt = 0
hotel_clusters = []
srch_adults_cnts = []
max_srch_adults = 0

srch_children_cnts = []
max_srch_chdrn = 0
max_hotel_clusters = 0
srch_rm_cnts = []
max_srch_rm = 0
chunkcount=0
for chunk in train:
	count = 0
	# date is yyyy-mm-dd
	for c in chunk['srch_ci']:
		try:
			if isnan(float(c)):
				checkin.append("0")
		except Exception as e:
			l = c.split("-")
			d0 = date(int(l[0]),int(l[1]),int(l[2]))
			checkin.append(d0)
	for c in chunk['srch_co']:
		try:
			if isnan(float(c)):
				checkout.append("0")
		except Exception as e:
			l = c.split("-")
			d0 = date(int(l[0]),int(l[1]),int(l[2]))
			checkout.append(d0)
	for c in chunk['is_booking']:
		is_bookings.append(c)

	for c in chunk['user_location_country']:
		user_location_countries.append(c)
		if c>max_user_loc_ctry:
			max_user_loc_ctry = c

	for c in chunk['user_location_region']:
		user_location_regions.append(c)
		if c>max_user_loc_rgn:
			max_user_loc_rgn = c
	for c in chunk['hotel_cluster']:
		hotel_clusters.append(c)
		if c>max_hotel_clusters:
			max_hotel_clusters = c
	for c in chunk['user_location_city']:
		user_location_cities.append(c)
		if c>max_user_loc_cty:
			max_user_loc_cty = c

	for c in chunk['srch_destination_type_id']:
		srch_destination_type_ids.append(c)
		if c>max_desttype_id:
			max_desttype_id = c

	for c in chunk['hotel_continent']:
		hotel_continents.append(c)
		if c>max_hotel_cnts:
			max_hotel_cnts = c

	for c in chunk['hotel_country']:
		hotel_countries.append(c)
		if c>max_hotel_cntr:
			max_hotel_cntr = c

	for c in chunk['hotel_market']:
		hotel_markets.append(c)
		if c>max_hotel_mrkt:
			max_hotel_mrkt = c

	for c in chunk['srch_adults_cnt']:
		srch_adults_cnts.append(c)
		if c>max_srch_adults:
			max_srch_adults = c

	for c in chunk['srch_children_cnt']:
		srch_children_cnts.append(c)
		if c>max_srch_chdrn:
			max_srch_chdrn = c

	for c in chunk['srch_rm_cnt']:
		srch_rm_cnts.append(c)
		if c>max_srch_rm:
			max_srch_rm = c
	
	s=""
	if chunkcount<10:
		s = "0"+str(chunkcount)
	else:
		s = str(chunkcount)
	with open(s+"ci","wb") as file:
		pickle.dump(checkin,file)
	with open(s+"co","wb") as file:
		pickle.dump(checkout,file)
	with open(s+"isbook","wb") as file:
		pickle.dump(is_bookings,file)
	is_bookings=[]
	with open(s+"user_country","wb") as file:
		pickle.dump(user_location_countries,file)
	user_location_countries=[]
	with open(s+"user_region","wb") as file:
		pickle.dump(user_location_regions,file)
	user_location_regions=[]
	with open(s+"user_city","wb") as file:
		pickle.dump(user_location_cities,file)
	user_location_cities=[]
	with open(s+"srch_type_ids","wb") as file:
		pickle.dump(srch_destination_type_ids,file)
	srch_destination_type_ids=[]
	with open(s+"hotel_conts","wb") as file:
		pickle.dump(hotel_continents,file)
	hotel_continents=[]
	with open(s+"hotel_contrs","wb") as file:
		pickle.dump(hotel_countries,file)
	hotel_countries=[]
	with open(s+"hotel_mrkts","wb") as file:
		pickle.dump(hotel_markets,file)
	hotel_markets=[]
	with open(s+"srch_adults","wb") as file:
		pickle.dump(srch_adults_cnts,file)
	srch_adults_cnts=[]
	with open(s+"srch_children","wb") as file:
		pickle.dump(srch_children_cnts,file)
	srch_children_cnts=[]
	with open(s+"srch_rm","wb") as file:
		pickle.dump(srch_rm_cnts,file)
	srch_rm_cnts=[]
	with open(s+"hotel_clusters","wb") as file:
		pickle.dump(hotel_clusters,file)
	n_records = len(hotel_clusters)
	hotel_clusters=[]
	nans = []
	print("no of records",n_records)
	for i in range(n_records):
		if checkout[i]=="0" or checkin[i]=="0":
			nans.append(i)
			continue
		delta = checkout[i] - checkin[i]
		day = delta.days
		if day>max_day:
			max_day = day
	with open(s+"nans","wb") as file:
		pickle.dump(nans,file)
	nans=[]
	checkout=[]
	checkin=[]
	chunkcount+=1
	print("processed",chunkcount)
	
print("max_hotel_clusters=",max_hotel_clusters)
print("max_day=",max_day)
print("max_user_loc_ctry=",max_user_loc_ctry)
print("max_user_loc_rgn=",max_user_loc_rgn)
print("max_user_loc_cty=",max_user_loc_cty)
print("max_desttype_id=",max_desttype_id)
print("max_hotel_cnts=",max_hotel_cnts)
print("max_hotel_cntr=",max_hotel_cntr)
print("max_hotel_mrkt=",max_hotel_mrkt)
print("max_srch_adults=",max_srch_adults)
print("max_srch_chdrn=",max_srch_chdrn)
print("max_srch_rm=",max_srch_rm)
import gc
gc.collect()
