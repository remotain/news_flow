from urllib.request import urlopen
import xml.etree.ElementTree as et
from pprint import pprint
import uuid
import time
import os
import datetime
from json import dumps
from kafka import KafkaProducer

RSS_URL = 'http://www.repubblica.it/rss/cronaca/rss2.0.xml?ref=RHFT'
TOPIC_NAME = 'repubblica_cronaca'
DATETIME_FORMAT = "%a, %d %b %Y %H:%M:%S %z"
TIME_ZONE = datetime.timezone(datetime.timedelta(0, 7200))

def touch(fname):
    open(fname, 'a').close()
    os.utime(fname, None)

def last_update(lock_file = 'last.update'):
    try:
        last_update = os.path.getmtime('last.update')
    except:
        last_update = 0
    return datetime.datetime.fromtimestamp(last_update, tz=TIME_ZONE)

# Connect to the Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# Main producer loop
while True:
    
    rss=urlopen(RSS_URL)
    tree = et.parse(rss)
    root = tree.getroot()

    count = 0
    """ 
    Loop over the item in the RSS, formate the message in JSON format,
    check if the message is more recent than the last update and if so
    send the message to the Kafka server
    """
    for item in root.getiterator(tag='item'):
    
        item = {
            'title'       : item.find('title').text,
            'link'        : item.find('link').text,
            'author'      : item.find('author').text,
            'category'    : item.find('category').text,
            'pubDate'     : item.find('pubDate').text,
            'description' : item.find('description').text,
            'guid'        : item.find('guid').text,
            'uuid'        : uuid.uuid3(uuid.NAMESPACE_URL, item.find('link').text)
        }
    
        pubDate =  datetime.datetime.strptime(item['pubDate'], DATETIME_FORMAT)
    
        # Skip if publication date is earlier than the last update
        # i.e. the item have been already processed.
        if pubDate < last_update():
            continue
    
        count += 1
    
        # Send the message to the Kafka server
        producer.send('TOPIC_NAME', value=item)
        #pprint(item)
    
    print('Update: %d new items since %s' % (count, last_update()))
    touch('last.update')
    time.sleep(60)