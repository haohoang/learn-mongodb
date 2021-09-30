from pymongo import MongoClient
import time 
import multiprocessing
import random

# client = MongoClient("mongodb://192.168.40.162:60000")
# db = client.hoang_test
# collection_name = db["profile_staging"]
# t1 = time.time()
# # collection 
# item_details = collection_name.find({ "gender.sources.value" : "F"})
# print("Time to query: ", (time.time() - t1))

# # i = 0
# # for item in item_details:
# #     print(item)
# #     i += 1
# #     if i > 10:
# #         break
# print(item_details)

documents = [{"i": i, 
              'username': 'user' + str(i),
              'age': random.sample(range(0, 100), 5)} for i in range(1000000)]

def insert_doc(chunk):
    client = MongoClient("mongodb://192.168.40.162:60000")
    db = client.mydb
    col = db.mycol
    col.insert_many(chunk)

chunk_size = 10000

def chunks(sequence):
    # Chunks of 1000 documents at a time.
    for j in range(0, len(sequence), chunk_size):
        yield sequence[j:j + chunk_size]

if __name__ == '__main__':
    time2s = time.time()
    pool = multiprocessing.Pool(processes=10)
    pool.map(insert_doc, chunks(documents))
    pool.close()
    pool.join()
    time2f = time.time()
    print(time2f - time2s)