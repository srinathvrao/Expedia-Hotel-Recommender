import csv
import pandas as pd
from datetime import date
chunksize = 10 ** 6
train = pd.read_csv("train.csv",chunksize=chunksize,usecols=["is_booking","orig_destination_distance",])
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
is_bookings=[]
srch_rm_cnts = []
min_srch_rm = 10 ** 6
max_srch_rm = 0
for chunk in train:
	chunk.fillna(chunk.mean(), inplace=True)
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


with open("features.csv", "w", newline="") as features_file:
	writer = csv.writer(features_file)
	features = ["stay_duration","travel_distance","user_location_countries","user_location_regions","user_location_cities","srch_destination_type_ids","hotel_continents","hotel_countries","hotel_markets","srch_adults_cnts","srch_children_cnts","srch_rm_cnts","is_bookings"]
	writer.writerow(features)
	for i in range(n_records):
		xi = date_diffs[i]
		norma = xi/max_day
		date_diffs[i] = norma

		xi = distances[i]
		norma = xi/max_dist
		distances[i] = norma

		xi = user_location_countries[i]
		norma = xi/max_user_loc_ctry
		user_location_countries[i] = norma

		xi = user_location_regions[i]
		norma = xi/max_user_loc_rgn
		user_location_regions[i] = norma

		xi = user_location_cities[i]
		norma = xi/max_user_loc_cty
		user_location_cities[i] = norma

		xi = srch_destination_type_ids[i]
		norma = xi/max_desttype_id
		srch_destination_type_ids[i] = norma

		xi = hotel_continents[i]
		norma = xi/max_hotel_cnts
		hotel_continents[i] = norma

		xi = hotel_countries[i]
		norma = xi/max_hotel_cntr
		hotel_countries[i] = norma

		xi = hotel_markets[i]
		norma = xi/max_hotel_mrkt
		hotel_markets[i] = norma

		xi = srch_adults_cnts[i]
		norma = xi/max_srch_adults
		srch_adults_cnts[i] = norma

		xi = srch_children_cnts[i]
		norma = xi/max_srch_chdrn
		srch_children_cnts[i] = norma

		xi = srch_rm_cnts[i]
		norma = xi/max_srch_rm
		srch_rm_cnts[i] = norma

		writer.writerow([date_diffs[i],distances[i],user_location_countries[i],user_location_regions[i],user_location_cities[i],srch_destination_type_ids[i],hotel_continents[i],hotel_countries[i],hotel_markets[i],srch_adults_cnts[i],srch_children_cnts[i],srch_rm_cnts[i],is_bookings[i]])



# ["likes_travel","stay_duration",

# "user_location_country","user_location_region","user_location_city",
# "srch_destination_type_id",
# "hotel_continent","hotel_country","hotel_market",
# "srch_adults_cnt","srch_children_cnt","srch_rm_cnt"]

# norma_distances, norma_stay_duration, 