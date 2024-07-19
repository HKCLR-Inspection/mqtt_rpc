import paho.mqtt.client as mqtt
from threading import Event
import json

class Callee:
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

        self.__register = {}

        connected_event = Event()
        def on_connect(client, userdata, flags, reason_code, properties):
            client.subscribe(self.__request_topic)

        # TODO: might need to modify this if multiple topics are to be used
        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            connected_event.set()

        def on_message(client, userdata, msg):
            str_payload = str(msg.payload, 'utf-8')       
            json_payload = json.loads(str_payload)
            method  = json_payload['method']
            tid     = json_payload['tid']

            if method in self.__register.keys():
                json_payload["data"] = self.__register[method](json_payload.copy())
            
            self.__client.publish(self.__reply_topic, json.dumps(json_payload))

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
        # wait until subscribed to request topic
        connected_event.wait()
    def __del__(self):
        self.__client.disconnect()
        self.__client.loop_stop()

    def register_call(self, method, callback):
        """Register a call

        Args:
            method (String): The field 'method' in the JSON
            callback (Callable): The function to be called
        """        
        self.__register[method] = callback
    