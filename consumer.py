import pika, os
from pymongo import MongoClient

#enum


# database connection
cluster = MongoClient("mongodb+srv://nirmal123:nirmal123@iot.aimnvsv.mongodb.net/?retryWrites=true&w=majority")
db = cluster["tata"]
collection = db["data"]

# Receive msg from MQ
url = os.environ.get("CLOUDAMQP_URL", "amqps://bwkukdzr:qe7hmCPfKlVPvB6az_5laaJToqZgDffy@puffin.rmq2.cloudamqp.com/bwkukdzr")
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

for method_frame, properties, body in channel.consume(queue="test_queue", auto_ack=True):
    data = str(body).removeprefix("b'[").removesuffix("]'").replace('"','').replace(" ","")
    data_l = data.split(",")
    c = data_l
    def validate(num):
        try:
            return int(num)
        except (ValueError, TypeError):
            try:
                return float(num)
            except (ValueError, TypeError):
                return num
    s = [validate(v) for v in c]
    header = s[0]
    timeoffset = s[1]
    depth = s[2]
    current = s[3]
    frequency = s[4]
    gain = s[5]
    signal = s[6]
    modelist = s[7]
    antennalist = s[8]
    dirlist = s[9]
    utilitylist = s[10]
    Depthsane = s[11]
    AutodepthOk = s[12]
    vertical = s[13]
    clipping = s[14]
    SigcompassLR = s[15]
    compassAngle = s[16]
    batteryLevel = s[17]
    AudioList = s[18]
    if ((0 <= timeoffset <= 65536) and (0 <= current <= 1000) and (0 <= signal <= 1000) and (-24 <= SigcompassLR <= 25)):
        Validation = "Pass"
    else:
        Validation = "Fail"


    data_list = {'Header': header, 'Time Offset': timeoffset,
             'Depth': depth, 'Current': current, 'Frequency': frequency,
             'Gain': gain, 'Signal': signal, 'Mode': modelist,
             'Antenna': antennalist, 'DE Direction': dirlist,
             'Utility Type': utilitylist, 'depthSane': Depthsane,
             'autodepthOK': AutodepthOk, 'Vertical': vertical,
             'Clipping': clipping, 'sig.compassLR': SigcompassLR, 'Compass Angle': compassAngle,
             'Battery Level': batteryLevel, 'Audio Volume': AudioList, 'Misc': s[19],'Validation': Validation,'Time': s[20], 'Date': s[21] , 'Device No': s[22]}


    collection.insert_one(data_list)
    print("Inserting to db", data_list)


channel.close()
connection.close()




