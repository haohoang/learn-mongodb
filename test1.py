import multiprocessing
from pymongo import MongoClient
import time
from functools import partial
from itertools import chain

document_ids = [i for i in range(1000000)]
# calculation function to be applied to every record in the database with an input
# def func(i):
#     # define client inside function
#     client = MongoClient("localhost", 27017, maxPoolSize=10000)
#     db = client.test
#     collection = db["exampleCollection"]
#     result = collection.find({"x": {"$lt": 400}}).count()
#     # do the calculation with ith element of collection(collection[i]
#     return result


# takes a list and integer n as input and returns generator objects of n lengths from that list
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


def calculate(chunk, input):

    # define client inside function
    client = MongoClient("localhost", 27017, maxPoolSize=10000)
    db = client.test
    users = db["users"]
    chunk_result_list = 0
    # loop over the id's in the chunk and do the calculation with each
    # chunk_result_list = users.count_documents({"i": {"$in": chunk}})
    # chunk_result_list = users.update_many({"i": {"$in": chunk}},
    #              [{"$set": {"convertAge2": {"$toInt": {'$divide': ["$convertAge", 10]}}}}])
    for id in chunk:
        users.update_many(
            {"i": id},
            [{"$set": {"convertAge3": {"$toInt": {"$divide": ["$convertAge", 10]}}}}],
        )
    #       #do the calculation with document collection.find_one(id)
    #       result = collection.count_documents({{"$in": []}})
    #       chunk_result_list.append(result)
    return chunk_result_list.modified_count


if __name__ == "__main__":
    # p = multiprocessing.Pool(processes=4)
    # data = p.map(func, [i for i in range(4)])
    # p.close()
    # print(data)
    # manager = multiprocessing.Manager()
    # return_dict = manager.dict()
    # jobs = []
    # for i in range(4):
    #     proc = multiprocessing.Process(target=func)
    #     jobs.append(proc)
    #     proc.start()
    # for proc in jobs:
    #     proc.join()
    # print("Done!")
    # pool object creation
    pool = multiprocessing.Pool(processes=4)  # spawn 8 processes
    calculate_partial = partial(
        calculate, input=input
    )  # partial function creation to allow input to be sent to the calculate function
    t1 = time.time()
    result = pool.map(
        calculate_partial, list(chunks(document_ids, 1000))
    )  # creates chunks of 1000 document id's
    pool.close()
    t2 = time.time()
    print("Time to query: ", (t2 - t1) / 60)
    # result is now a list of lists which needs to be converted into a single list
    # result_final = list(chain.from_iterable([r.items() for r in result]))
    print(result[0])
    print(sum(result))
