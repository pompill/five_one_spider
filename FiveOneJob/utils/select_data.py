import pymongo


def parse():
    client = pymongo.MongoClient(host='localhost', port=27017)
    client.admin.authenticate("", "")
    db = client.fwwb
    city_num = db.LiePin_city
    data = city_num.find()
    return data