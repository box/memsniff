
# coding: utf-8

# In[99]:


from kafka import KafkaConsumer, KafkaClient, SimpleClient
from kafka.cluster import ClusterMetadata
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import TopicPartition, OffsetAndTimestamp, OffsetRequestPayload
import time
from operator import itemgetter
import requests
import datetime
import json


# In[98]:


def get_end_offsets(topic):
    client = SimpleClient([
        "kafka1.internal.zomans.com:9092",
        "kafka2.internal.zomans.com:9092",
        "kafka3.internal.zomans.com:9092",
    ])

    partitions = client.topic_partitions[topic]
    offset_requests = [
        OffsetRequestPayload(topic, p, -1, 1)
        for p in partitions.keys()
    ]

    offsets_responses = client.send_offset_request(offset_requests)
    return {
        TopicPartition(topic=offset_response_payload.topic, partition=offset_response_payload.partition):
            offset_response_payload.offsets[0]
        for offset_response_payload in offsets_responses
    }


# In[3]:
def main():

    topic = "logs.scripts.memsniff-events-v2"
    client = SimpleClient([
        "kafka1.internal.zomans.com:9092",
        "kafka2.internal.zomans.com:9092",
        "kafka3.internal.zomans.com:9092",
    ])
    end_offsets = get_end_offsets(topic)
    msgs = []
    for partition in client.topic_partitions[topic].keys():
        consumer = KafkaConsumer(
            client_id='zmemsniff-kafka-consumer-client-1',
            bootstrap_servers=[
                "kafka1.internal.zomans.com:9092",
                "kafka2.internal.zomans.com:9092",
                "kafka3.internal.zomans.com:9092",
            ],
            auto_offset_reset='earliest',
        )
        topic_partition = TopicPartition(topic=topic, partition=partition)
        consumer.assign([
            topic_partition,
        ])
        for msg in consumer:
            if consumer.position(partition=topic_partition) >= end_offsets[topic_partition]:
                break
            msgs.append(msg)
        consumer.close()


    # In[81]:


    today = int(datetime.datetime.now().timestamp()) * 1000
    yesterday = int(datetime.datetime.now().timestamp() - 24 * 60 * 60) * 1000


    # In[86]:


    filtered_msgs = [msg for msg in msgs if yesterday <= msg.timestamp <= today]


    # In[87]:


    global_aggregates = {
        'avg(size)': {},
        'cnt(size)': {},
        'sum(size)': {},
    }
    for msg in filtered_msgs:
        event = json.loads(msg.value)
        for item in event['event_message']['analysis_report']['rows']:
            if item['key'][0] not in global_aggregates[event['event_message']['aggregate_column_name']]:
                global_aggregates[event['event_message']['aggregate_column_name']][item['key'][0]] = []
            global_aggregates[event['event_message']['aggregate_column_name']][item['key'][0]].append({
                event['event_message']['analysis_report']['val_col_names'][idx]: val
                for idx, val in enumerate(item['values'])
            })


    # In[88]:


    global_aggregate_items = {
        'avg(size)': [],
        'cnt(size)': [],
        'sum(size)': [],
    }
    for aggregate_type in global_aggregates.keys():
        for key, value in global_aggregates[aggregate_type].items():
            gagts = {
                'avg(size)': 0,
                'cnt(size)': 0,
                'sum(size)': 0,
            }
            for agt in gagts.keys():
                lagts = list(map(itemgetter(agt), value))
                gagts[agt] = int(sum(lagts) / len(lagts))
            global_aggregate_items[aggregate_type].append({
                'key': key,
                **gagts,
            })


    # In[89]:


    for agt in global_aggregate_items.keys():
        global_aggregate_items[agt] = sorted(
            global_aggregate_items[agt],
            key=itemgetter(agt),
            reverse=True,
        )[:5]


    # In[90]:


    print(f'{"key":<75} {"bytes":<10} {"rpm":<10} {"kbps":<10}')
    print()
    for agt_item in global_aggregate_items['avg(size)']:
        print(f'{agt_item["key"][19:]:<75} {agt_item["avg(size)"]:<10} {int(agt_item["cnt(size)"] * 2):<10} {int(agt_item["sum(size)"] / (30 * 1024)):<10}')

    print()
    print()
    print()
    print()
    print(f'{"key":<75} {"bytes":<10} {"rpm":<10} {"kbps":<10}')
    print()
    for agt_item in global_aggregate_items['cnt(size)']:
        print(f'{agt_item["key"][19:]:<75} {agt_item["avg(size)"]:<10} {int(agt_item["cnt(size)"] * 2):<10} {int(agt_item["sum(size)"] / (30 * 1024)):<10}')

    print()
    print()
    print()
    print()
    print(f'{"key":<75} {"bytes":<10} {"rpm":<10} {"kbps":<10}')
    for agt_item in global_aggregate_items['sum(size)']:
        print(f'{agt_item["key"][19:]:<75} {agt_item["avg(size)"]:<10} {int(agt_item["cnt(size)"] * 2):<10} {int(agt_item["sum(size)"] / (30 * 1024)):<10}')


    # In[126]:


    markdown_items = []
    markdown_items.append(f'### big keys (kb)')
    markdown_items.append('==================')
    for agt_item in global_aggregate_items['avg(size)']:
        markdown_items.append(f'**{agt_item["key"][19:]}**')
        markdown_items.append(f'> {agt_item["cnt(size)"] * 2}rpm {int(agt_item["avg(size)"] / 1024)}kb {int(agt_item["sum(size)"] / (30 * 1024)):}kbps')
        markdown_items.append('\n')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')

    markdown_items.append(f'### keys with highest throughput (rpm)')
    markdown_items.append('==================')
    for agt_item in global_aggregate_items['cnt(size)']:
        markdown_items.append(f'**{agt_item["key"][19:]}**')
        markdown_items.append(f'> {agt_item["cnt(size)"] * 2}rpm {int(agt_item["avg(size)"] / 1024)}kb {int(agt_item["sum(size)"] / (30 * 1024)):}kbps')
        markdown_items.append('\n')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
    markdown_items.append('\n &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')

    markdown_items.append(f'### keys consuming high bandwidth (kbps)')
    markdown_items.append('==================')
    for agt_item in global_aggregate_items['sum(size)']:
        markdown_items.append(f'**{agt_item["key"][19:]}**')
        markdown_items.append(f'> {agt_item["cnt(size)"] * 2}rpm {int(agt_item["avg(size)"] / 1024)}kb {int(agt_item["sum(size)"] / (30 * 1024)):}kbps')
        markdown_items.append('\n')


    # In[127]:


    markdown_msg = '\n'.join(markdown_items)


    # In[128]:


    resp = requests.post(
        'https://oapi.dingtalk.com/robot/send?access_token=8a8a3ee885965a13b2d4f3e6942e59b402dec4959fb28e99b3d5fabdaa909f93',
        headers={
            'Content-Type': 'application/json;charset=utf-8',
        },
        json={
            'msgtype': 'markdown',
            'markdown': {
                'title': "Memcache Alert",
                'text': markdown_msg,
            }
        }
    )



def lambda_handler(event, context):
    # TODO implement
    print(event)
    print(context)
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


