import csv
import pandas as pd
from datetime import date
chunksize = 10 ** 6
train = pd.read_csv("train.csv",chunksize=chunksize)
sumdist=0
checkin=[]
checkout=[]
def avg(l):
	return sum(l)/len(l)

date_diffs=[]
min_day=10 ** 6
max_day=0

distances = []
min_dist=10 ** 6
max_dist=0

user_location_countries = []
min_user_loc_ctry=10 ** 6
max_user_loc_ctry=0

user_location_regions = []
min_user_loc_rgn =10 ** 6
max_user_loc_rgn = 0

user_location_cities = []
min_user_loc_cty =10 ** 6
max_user_loc_cty = 0

srch_destination_type_ids = []
min_desttype_id =10 ** 6
max_desttype_id = 0

hotel_continents = []
min_hotel_cnts = 10 ** 6
max_hotel_cnts = 0

hotel_countries = []
min_hotel_cntr = 10 ** 6
max_hotel_cntr = 0

hotel_markets = []
min_hotel_mrkt = 10 ** 6
max_hotel_mrkt = 0

srch_adults_cnts = []
min_srch_adults = 10 ** 6
max_srch_adults = 0

srch_children_cnts = []
min_srch_chdrn = 10 ** 6
max_srch_chdrn = 0

srch_rm_cnts = []
min_srch_rm = 10 ** 6
max_srch_rm = 0

is_bookings = []
print(train["orig_destination_distance"].mean())
exit(0)
for chunk in train:
	count = 0
	for c in chunk['is_booking']:
		is_bookings.append(c)
		count+=1
		if count==500:
			count=0
			break
	for c in chunk['orig_destination_distance']:
		print(c)
		distances.append(c)
		if c>max_dist:
			max_dist = c
		if c<min_dist:
			min_dist = c
		count+=1
		if count==50:
			exit(0)
		if count==500:
			count=0
			break


	for c in chunk['user_location_country']:
		user_location_countries.append(c)
		if c>max_user_loc_ctry:
			max_user_loc_ctry = c
		if c<min_user_loc_ctry:
			min_user_loc_ctry = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['user_location_region']:
		user_location_regions.append(c)
		if c>max_user_loc_rgn:
			max_user_loc_rgn = c
		if c<min_user_loc_rgn:
			min_user_loc_rgn = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['user_location_city']:
		user_location_cities.append(c)
		if c>max_user_loc_cty:
			max_user_loc_cty = c
		if c<min_user_loc_cty:
			min_user_loc_cty = c

		count+=1
		if count==500:
			count=0
			break

	for c in chunk['srch_destination_type_id']:
		srch_destination_type_ids.append(c)
		if c>max_desttype_id:
			max_desttype_id = c
		if c<min_desttype_id:
			min_desttype_id = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['hotel_continent']:
		hotel_continents.append(c)
		if c>max_hotel_cnts:
			max_hotel_cnts = c
		if c<min_hotel_cnts:
			min_hotel_cnts = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['hotel_country']:
		hotel_countries.append(c)
		if c>max_hotel_cntr:
			max_hotel_cntr = c
		if c<min_hotel_cntr:
			min_hotel_cntr = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['hotel_market']:
		hotel_markets.append(c)
		if c>max_hotel_mrkt:
			max_hotel_mrkt = c
		if c<min_hotel_mrkt:
			min_hotel_mrkt = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['srch_adults_cnt']:
		srch_adults_cnts.append(c)
		if c>max_srch_adults:
			max_srch_adults = c
		if c<min_srch_adults:
			min_srch_adults = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['srch_children_cnt']:
		srch_children_cnts.append(c)
		if c>max_srch_chdrn:
			max_srch_chdrn = c
		if c<min_srch_chdrn:
			min_srch_chdrn = c
		count+=1
		if count==500:
			count=0
			break

	for c in chunk['srch_rm_cnt']:
		srch_rm_cnts.append(c)
		if c>max_srch_rm:
			max_srch_rm = c
		if c<min_srch_rm:
			min_srch_rm = c
		count+=1
		if count==500:
			count=0
			break

	# date is yyyy-mm-dd
	for c in chunk['srch_ci']:
		l = c.split("-")
		d0 = date(int(l[0]),int(l[1]),int(l[2]))
		checkin.append(d0)
		count+=1
		if count==500:
			count=0
			break
	for c in chunk['srch_co']:
		l = c.split("-")
		d0 = date(int(l[0]),int(l[1]),int(l[2]))
		checkout.append(d0)
		count+=1
		if count==500:
			count=0
			break
	break
n_records = len(checkin)
print("no of records",n_records)
for i in range(n_records):
	delta = checkout[i] - checkin[i]
	day = delta.days
	if day>max_day:
		max_day = day
	if day<min_day:
		min_day = day
	date_diffs.append(day)

print("distances..",len(distances),sum(distances[:250]))
import numpy as np
avgdist = np.nanmean(distances)

xavg = np.nanmean(date_diffs)
max_min_diffs = max_day-min_day
max_min_dist = max_dist-min_dist

avg_user_loc_ctry = np.nanmean(user_location_countries)
max_min_usr_loc_ctry = max_user_loc_ctry - min_user_loc_ctry

avg_user_loc_rgn = np.nanmean(user_location_regions)
max_min_usr_loc_rgn = max_user_loc_rgn - min_user_loc_rgn

avg_user_loc_cty = np.nanmean(user_location_cities)
max_min_usr_loc_cty = max_user_loc_cty - min_user_loc_cty

avg_dstn_type_id = np.nanmean(srch_destination_type_ids)
max_min_dstn_type_id = max_desttype_id - min_desttype_id

avg_hotel_continents = np.nanmean(hotel_continents)
max_min_hotel_continent = max_hotel_cnts - min_hotel_cnts

avg_hotel_countries = np.nanmean(hotel_countries)
max_min_hotel_cntry = max_hotel_cntr - min_hotel_cntr

avg_hotel_markets = np.nanmean(hotel_markets)
max_min_hotel_mrkts = max_hotel_mrkt - min_hotel_mrkt

avg_srch_adults_cnts = np.nanmean(srch_adults_cnts)
max_min_srch_adults_cnts = max_srch_adults - min_srch_adults

avg_srch_children_cnts = np.nanmean(srch_children_cnts)
max_min_srch_children_cnts = max_srch_chdrn - min_srch_chdrn

avg_srch_rm_cnts = np.nanmean(srch_rm_cnts)
max_min_srch_rm_cnts = max_srch_rm - min_srch_rm

with open("features.csv", "w", newline="") as features_file:
	writer = csv.writer(features_file)
	features = ["stay_duration","travel_distance","user_location_countries","user_location_regions","user_location_cities","srch_destination_type_ids","hotel_continents","hotel_countries","hotel_markets","srch_adults_cnts","srch_children_cnts","srch_rm_cnts","is_bookings"]
	writer.writerow(features)
	for i in range(n_records):
		xi = date_diffs[i]
		norma = (xi-xavg)/max_min_diffs
		date_diffs[i] = norma

		xi = distances[i]
		norma = (xi-avgdist)/max_min_dist
		distances[i] = norma

		xi = user_location_countries[i]
		norma = (xi-avg_user_loc_ctry)/max_min_usr_loc_ctry
		user_location_countries[i] = norma

		xi = user_location_regions[i]
		norma = (xi-avg_user_loc_rgn)/max_min_usr_loc_rgn
		user_location_regions[i] = norma

		xi = user_location_cities[i]
		norma = (xi-avg_user_loc_cty)/max_min_usr_loc_cty
		user_location_cities[i] = norma

		xi = srch_destination_type_ids[i]
		norma = (xi-avg_dstn_type_id)/max_min_dstn_type_id
		srch_destination_type_ids[i] = norma

		xi = hotel_continents[i]
		norma = (xi-avg_hotel_continents)/max_min_hotel_continent
		hotel_continents[i] = norma

		xi = hotel_countries[i]
		norma = (xi-avg_hotel_countries)/max_min_hotel_cntry
		hotel_countries[i] = norma

		xi = hotel_markets[i]
		norma = (xi-avg_hotel_markets)/max_min_hotel_mrkts
		hotel_markets[i] = norma

		xi = srch_adults_cnts[i]
		norma = (xi-avg_srch_adults_cnts)/max_min_srch_adults_cnts
		srch_adults_cnts[i] = norma

		xi = srch_children_cnts[i]
		norma = (xi-avg_srch_children_cnts)/max_min_srch_children_cnts
		srch_children_cnts[i] = norma

		xi = srch_rm_cnts[i]
		norma = (xi-avg_srch_rm_cnts)/max_min_srch_rm_cnts
		srch_rm_cnts[i] = norma

		writer.writerow([date_diffs[i],distances[i],user_location_countries[i],user_location_regions[i],user_location_cities[i],srch_destination_type_ids[i],hotel_continents[i],hotel_countries[i],hotel_markets[i],srch_adults_cnts[i],srch_children_cnts[i],srch_rm_cnts[i],is_bookings[i]])



# ["likes_travel","stay_duration",

# "user_location_country","user_location_region","user_location_city",
# "srch_destination_type_id",
# "hotel_continent","hotel_country","hotel_market",
# "srch_adults_cnt","srch_children_cnt","srch_rm_cnt"]

# norma_distances, norma_stay_duration, 