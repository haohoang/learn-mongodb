from pymongo import MongoClient
import time 

client = MongoClient("mongodb://192.168.40.162:60000")
db = client.hoang_test
collection_name = db["profile_staging"]
t1 = time.time()
# collection 
item_details = collection_name.find({ "gender.sources.value" : "F"})
print("Time to query: ", (time.time() - t1))

# i = 0
# for item in item_details:
#     print(item)
#     i += 1
#     if i > 10:
#         break
print(item_details)
