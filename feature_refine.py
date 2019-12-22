import pickle
import os
s=""
omits=0

max_user_id= 1198785
max_srch_dstn_id= 65107
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

for chunkcount in range(1,39):
    nans=[]
    if chunkcount<10:
        s = "features/0"+str(chunkcount)
    else:
        s = "features/"+str(chunkcount)
    user_ids = []
    with open(s+"userid","rb") as file:
        user_ids = pickle.load(file)
    srch_dstn_ids = []
    with open(s+"srch_dstn_id","rb") as file:
        srch_dstn_ids = pickle.load(file)
    checkin=[]
    with open(s+"ci","rb") as file:
        checkin = pickle.load(file)
    checkout=[]
    with open(s+"co","rb") as file:
        checkout = pickle.load(file)
    is_bookings=[]
    with open(s+"isbook","rb") as file:
        is_bookings = pickle.load(file)
    user_location_countries = []
    with open(s+"user_country","rb") as file:
        user_location_countries = pickle.load(file)
    user_location_regions = []
    with open(s+"user_region","rb") as file:
        user_location_regions = pickle.load(file)
    user_location_cities = []
    with open(s+"user_city","rb") as file:
        user_location_cities = pickle.load(file)
    srch_destination_type_ids = []
    with open(s+"srch_type_ids","rb") as file:
        srch_destination_type_ids = pickle.load(file)
    hotel_continents = []
    with open(s+"hotel_conts","rb") as file:
        hotel_continents = pickle.load(file)
    hotel_countries = []
    with open(s+"hotel_contrs","rb") as file:
        hotel_countries = pickle.load(file)
    hotel_markets = []
    with open(s+"hotel_mrkts","rb") as file:
        hotel_markets = pickle.load(file)
    srch_adults_cnts = []
    with open(s+"srch_adults","rb") as file:
        srch_adults_cnts = pickle.load(file)
    srch_children_cnts = []
    with open(s+"srch_children","rb") as file:
        srch_children_cnts = pickle.load(file)
    srch_rm_cnts = []
    with open(s+"srch_rm","rb") as file:
        srch_rm_cnts = pickle.load(file)
    hotel_clusters = []
    with open(s+"hotel_clusters","rb") as file:
        hotel_clusters = pickle.load(file)
    n = len(checkin)
    chunk_omits = 0
    stay_duration = []
    is_bookings2=[]
    user_location_countries2=[]
    user_location_regions2=[]
    user_location_cities2=[]
    srch_destination_type_ids2=[]
    hotel_continents2=[]
    hotel_countries2=[]
    hotel_markets2=[]
    srch_adults_cnts2=[]
    srch_children_cnts2=[]
    srch_rm_cnts2=[]
    hotel_clusters2=[]
    user_ids2 = []
    srch_dstn_ids2 = []
    for i in range(n):
        if checkin[i]!="0" and checkout[i]!="0":
            delta = checkout[i] - checkin[i]
            stay_duration.append(delta.days)
            if delta.days > max_day:
                max_day = delta.days
            is_bookings2.append(is_bookings[i])
            user_location_countries2.append(user_location_countries[i])
            user_location_regions2.append(user_location_regions[i])
            user_location_cities2.append(user_location_cities[i])
            srch_destination_type_ids2.append(srch_destination_type_ids[i])
            hotel_continents2.append(hotel_continents[i])
            hotel_countries2.append(hotel_countries[i])
            hotel_markets2.append(hotel_markets[i])
            srch_adults_cnts2.append(srch_adults_cnts[i])
            srch_children_cnts2.append(srch_children_cnts[i])
            srch_rm_cnts2.append(srch_rm_cnts[i])
            hotel_clusters2.append(hotel_clusters[i])
            user_ids2.append(user_ids[i])
            srch_dstn_ids2.append(srch_dstn_ids[i])
    user_ids=[]
    srch_dstn_ids=[]
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
    print(len(user_ids2))
    print(len(srch_dstn_ids2))
    print(len(is_bookings2))
    print(len(user_location_countries2))
    print(len(user_location_regions2))
    print(len(user_location_cities2))
    print(len(srch_destination_type_ids2))
    print(len(hotel_continents2))
    print(len(hotel_countries2))
    print(len(hotel_markets2))
    print(len(srch_adults_cnts2))
    print(len(srch_children_cnts2))
    print(len(srch_rm_cnts2))
    print(len(hotel_clusters2))
    with open(s+"stay","wb") as file:
        stay_duration = [(x/max_day) for x in stay_duration]
        pickle.dump(stay_duration,file)
    os.remove(s+"userid")
    with open(s+"userid","wb") as file:
        user_ids2 = [(x/max_user_id) for x in user_ids2]
        pickle.dump(user_ids2,file)
    os.remove(s+"srch_dstn_id")
    with open(s+"srch_dstn_id","wb") as file:
        srch_dstn_ids2 = [(x/max_srch_dstn_id) for x in srch_dstn_ids2]
        pickle.dump(srch_dstn_ids2,file)
    os.remove(s+"hotel_clusters")
    with open(s+"hotel_clusters","wb") as file:
        pickle.dump(hotel_clusters2,file)
    os.remove(s+"isbook")
    with open(s+"isbook","wb") as file:
        pickle.dump(is_bookings2,file)
    os.remove(s+"user_country")
    with open(s+"user_country","wb") as file:
        user_location_countries2 = [x/max_user_loc_ctry for x in user_location_countries2]
        pickle.dump(user_location_countries2,file)
    os.remove(s+"user_region")
    with open(s+"user_region","wb") as file:
        user_location_regions2 = [x/max_user_loc_rgn for x in user_location_regions2]
        pickle.dump(user_location_regions2,file)
    os.remove(s+"user_city")
    with open(s+"user_city","wb") as file:
        user_location_cities2 = [x/max_user_loc_cty for x in user_location_cities2]
        pickle.dump(user_location_cities2,file)
    os.remove(s+"srch_type_ids")
    with open(s+"srch_type_ids","wb") as file:
        srch_destination_type_ids2 =  [x/max_desttype_id for x in srch_destination_type_ids2]
        pickle.dump(srch_destination_type_ids2,file)
    os.remove(s+"hotel_conts")
    with open(s+"hotel_conts","wb") as file:
        hotel_continents2 = [x/max_hotel_cnts for x in hotel_continents2]
        pickle.dump(hotel_continents2,file)
    os.remove(s+"hotel_contrs")
    with open(s+"hotel_contrs","wb") as file:
        hotel_countries2 = [x/max_hotel_cntr for x in hotel_countries2]
        pickle.dump(hotel_countries2,file)
    os.remove(s+"hotel_mrkts")
    with open(s+"hotel_mrkts","wb") as file:
        hotel_markets2 = [x/max_hotel_mrkt for x in hotel_markets2]
        pickle.dump(hotel_markets2,file)
    os.remove(s+"srch_adults")
    with open(s+"srch_adults","wb") as file:
        srch_adults_cnts2 = [x/max_srch_adults for x in srch_adults_cnts2]
        pickle.dump(srch_adults_cnts2,file)
    os.remove(s+"srch_children")
    with open(s+"srch_children","wb") as file:
        srch_children_cnts2 = [x/max_srch_chdrn for x in srch_children_cnts2]
        pickle.dump(srch_children_cnts2,file)
    os.remove(s+"srch_rm")
    with open(s+"srch_rm","wb") as file:
        srch_rm_cnts2 = [x/max_srch_rm for x in srch_rm_cnts2]
        pickle.dump(srch_rm_cnts2,file)
    omits += chunk_omits
    print(chunkcount,"processed. Omitted:",chunk_omits,omits)
    stay_duration = []
    user_ids2 = []
    srch_dstn_ids2 = []
    is_bookings2=[]
    user_location_countries2=[]
    user_location_regions2=[]
    user_location_cities2=[]
    srch_destination_type_ids2=[]
    hotel_continents2=[]
    hotel_countries2=[]
    hotel_markets2=[]
    srch_adults_cnts2=[]
    srch_children_cnts2=[]
    srch_rm_cnts2=[]
    hotel_clusters2=[]
