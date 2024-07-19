import paho.mqtt.client as mqtt
from queue import Queue
from queue import Empty

from threading import Event
from threading import Thread

import time
import json

class RPCTimeoutException(Exception):
    """Exception raised when a RPC timeout
    TODO: Stuff whatever you need into the exception
    """    
    def __init__(self, method, time_stamp, wait_time):
        self.method     = method
        self.time_stamp = time_stamp
        self.wait_time  = wait_time
        super().__init__("RPC time out")

class Caller:
    """A utility class for MQTT-based remote procedure call;
    The implementation is based on paho-mqtt, pls see https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html
    """    
    def __init__(self, host, port, username, default_request_topic, default_reply_topic):
        """A blocking constructor. Returns only if TCP connection is established

        Args:
            host (String):          address of callee
            port (int):             port number of callee
            username (String):      user name for MQTT client, see paho-mqtt documentation
            request_topic (String): default topic name to publish request of services
            reply_topic (String):   default topic name to receive reply
        """        
        self.__request_topic    = default_request_topic
        self.__reply_topic      = default_reply_topic

        self.__client           = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.__client.username  = username

        self.__reply_dict = {}

        connected_event = Event()
        def on_connect(client, userdata, flags, reason_code, properties):
            client.subscribe(self.__reply_topic)

        # TODO: might need to modify this if multiple topics are to be used
        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            connected_event.set()

        def on_message(client, userdata, msg):
            str_payload = str(msg.payload, 'utf-8')       
            json_payload = json.loads(str_payload)
            method  = json_payload['method']
            tid     = json_payload['tid']

            # TODO: I am not so sure about this tid & bid, consult DJI about 'tid' and 'bid'
            if method in self.__reply_dict.keys() and tid in self.__reply_dict[method].keys():
                self.__reply_dict[method][tid].put(json_payload)

        def on_disconnect(client, userdata, rc):
            connected_event.clear()    
        
        self.__client.on_connect    = on_connect
        self.__client.on_message    = on_message
        self.__client.on_disconnect = on_disconnect
        self.__client.on_subscribe  = on_subscribe

        self.__client.connect(
            host = host,
            port = port
        )

        # start a new thread here
        self.__client.loop_start()
        # wait until connected
        connected_event.wait()

    def call(self, payload, timeout=None):
        """Blocking call, returns return from callee;
        If timeout is a positive number, it blocks at most **timeout** seconds and raises the Empty exception if no item

        Args:
            payload (String): JSON String to send through MQTT; See https://developer.dji.com/doc/cloud-api-tutorial/cn/api-reference/dock-to-cloud/mqtt/topic-definition.html
            timeout (int): Seconds to wait before it raises a RPCTimeoutException
        """
        json_payload = json.loads(payload)
        method  = json_payload['method']
        tid     = json_payload['tid']

        if not method in self.__reply_dict.keys():
            self.__reply_dict[method] = {}
        self.__reply_dict[method][tid] = Queue(1)

        self.__client.publish(self.__request_topic, payload)
        try:
            ret = self.__reply_dict[method][tid].get(block=True, timeout=timeout)
        except Empty as e:
            raise RPCTimeoutException(
                method=json_payload['method'],
                time_stamp=json_payload['timestamp'],
                wait_time=timeout)

        return ret
    
    def call_async(self, payload, callback):
        json_payload = json.loads(payload)
        method  = json_payload['method']
        tid     = json_payload['tid']

        if not method in self.__reply_dict.keys():
            self.__reply_dict[method] = {}
        self.__reply_dict[method][tid] = Queue(1)

        self.__client.publish(self.__request_topic, payload)
        def callback_invoker():
            ret = self.__reply_dict[method][tid].get(block=True)
            callback(ret)
        t = Thread(target=callback_invoker)
        t.start()


    def __del__(self):
        self.__client.disconnect()
        self.__client.loop_stop()

# The following code is for demonstration only, remove at you convenience
if __name__== '__main__':
    caller = Caller(
        host            ="broker", 
        port            =1883, 
        username        ="test", 
        default_request_topic   ="/local/request", 
        default_reply_topic     ="/local/reply")
    
    def callback(json_obj):
        print("Reply ({}): {}".format(json_obj['tid'], json_obj['data']['msg']))

    for i in range(10):
        payload = {
            "method" : "echo",
            "tid" : i,
            "data" : {"msg" : "Hello"}
        }
        str_payload = json.dumps(payload)
        caller.call_async(payload=str_payload, callback=callback)
        payload = {
            "method" : "ping",
            "tid" : i,
            "data" : {"msg" : "World"}
        }
        str_payload = json.dumps(payload)
        caller.call_async(payload=str_payload, callback=callback)
        