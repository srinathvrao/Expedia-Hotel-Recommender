import pickle
import os
s=""
omits=0

stay=[]
is_bookings=[]
user_location_countries = []
user_location_regions = []
user_location_cities = []
srch_destination_type_ids = []
hotel_continents = []
hotel_countries = []
hotel_markets = []
srch_adults_cnts = []
srch_children_cnts = []
srch_rm_cnts = []
hotel_clusters = []
max_hotel_clusters= 99
max_day= 516
max_user_loc_ctry= 239
max_user_loc_rgn= 1027
max_user_loc_cty= 56508
max_desttype_id= 9
max_hotel_cnts= 6
max_hotel_cntr= 212
max_hotel_mrkt= 2117
max_srch_adults= 9
max_srch_chdrn= 9
max_srch_rm= 8
nans=[]
for chunkcount in range(1,39):
    if chunkcount<10:
        s = "features/0"+str(chunkcount)
    else:
        s = "features/"+str(chunkcount)
    with open(s+"nans","rb") as file:
        nans.extend(pickle.load(file))
    print("nans",len(nans))
    with open(s+"stay","rb") as file:
        stay.extend(pickle.load(file))
    with open(s+"isbook","rb") as file:
        is_bookings.extend(pickle.load(file))
    with open(s+"user_country","rb") as file:
        user_location_countries.extend(pickle.load(file))
    with open(s+"user_region","rb") as file:
        user_location_regions.extend(pickle.load(file))
    with open(s+"user_city","rb") as file:
        user_location_cities.extend(pickle.load(file))
    with open(s+"srch_type_ids","rb") as file:
        srch_destination_type_ids.extend(pickle.load(file))
    with open(s+"hotel_conts","rb") as file:
        hotel_continents.extend(pickle.load(file))
    with open(s+"hotel_contrs","rb") as file:
        hotel_countries.extend(pickle.load(file))
    with open(s+"hotel_mrkts","rb") as file:
        hotel_markets.extend(pickle.load(file))
    with open(s+"srch_adults","rb") as file:
        srch_adults_cnts.extend(pickle.load(file))
    with open(s+"srch_children","rb") as file:
        srch_children_cnts.extend(pickle.load(file))
    with open(s+"srch_rm","rb") as file:
        srch_rm_cnts.extend(pickle.load(file))
    with open(s+"hotel_clusters","rb") as file:
        hotel_clusters.extend(pickle.load(file))
    print("processed",chunkcount)
    break
n = len(is_bookings)
print(len(is_bookings))
print(len(user_location_countries))
print(len(user_location_regions))
print(len(user_location_cities))
print(len(srch_destination_type_ids))
print(len(hotel_continents))
print(len(hotel_countries))
print(len(hotel_markets))
print(len(srch_adults_cnts))
print(len(srch_children_cnts))
print(len(srch_rm_cnts))
print(len(hotel_clusters))
featurevects = []
outputs = []
c=0
for i in range(n):
    if i in nans:
        continue
    c+=1
    print(user_location_countries[i],user_location_countries[i]/max_user_loc_ctry)
    if c==10:
        break
print(c)
'''
    vect = [user_location_countries[i],user_location_regions[i],user_location_cities[i],srch_destination_type_ids[i],hotel_continents[i],hotel_countries[i],hotel_markets[i],srch_adults_cnts[i],srch_children_cnts[i],srch_rm_cnts[i]]
    output = [0]*100
    if is_bookings[i]==1:
        output[hotel_clusters[i]] = 1
    else:
        output[hotel_clusters[i]] = 0.8
    featurevects.append(vect)
    outputs.append(output)
is_bookings=[]
user_location_countries=[]
user_location_regions=[]
user_location_cities=[]
srch_destination_type_ids=[]
hotel_continents=[]
hotel_countries=[]
hotel_markets=[]
srch_adults_cnts=[]
srch_children_cnts=[]
srch_rm_cnts=[]
hotel_clusters=[]
'''