# from typing import Union
#
# from fastapi import FastAPI, BackgroundTasks
# # from page_views import PageView, count_page_views
# from kafka import KafkaProducer
# import json
#
# producer = KafkaProducer(bootstrap_servers=['localhost:29092'])
# # with open('inputfile.txt') as f:
# #      lines = f.readlines()
#
# # for line in lines:
# #     producer.send('quickstart-events', json.dumps(line).encode('utf-8'))
#
# app = FastAPI()
# #
#
# async def send_value(a, b) -> None:
#     print(a, b)
#     producer.send('page_views', json.dumps({"id": "fo11o", "user": "111bar"}).encode('utf-8'))
#
#     # await count_page_views.ask(PageView(id="foo", user="bar"))
#
#
# @app.get("/")
# async def read_root():


#     return {"Hello": "World"}
#
#
# @app.get("/items")
# async def read_item(background_tasks: BackgroundTasks):
#     a, b = 2, 3
#     res = {"a": a, "b": b}
#     background_tasks.add_task(send_value, a, b)
#     return res


from confluent_kafka import Producer
import uuid
import json
import time
import random


def generate_random_time_series_data(count=0):
    new_message = {"hits":random.randint(10,100),"timestamp":time.time(),"userId":random.randint(15,20)}
    return count+1,new_message


def kafka_producer():
    bootstrap_servers = "localhost:29092"
    topic = "hit_count"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    total = 10
    count =0
    while total:
        count,base_message = generate_random_time_series_data(count)
        total-=1

        record_key = str(uuid.uuid4())
        record_value = json.dumps(base_message)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)

    p.flush()
    print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))

if __name__ == "__main__":
  kafka_producer()
