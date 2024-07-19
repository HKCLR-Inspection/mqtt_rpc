# A sample simply demonstrates how a callee in the RPC mechanism works

from Callee import Callee
from threading import Event

def echo(json_payload):
    """This is where you define a procedure to be called remotely

    Args:
        json_payload (dict): A JSON object, containing:
        transaction id,
        business id,
        timestamp, etc.
        Pls see https://developer.dji.com/doc/cloud-api-tutorial/cn/api-reference/dock-to-cloud/mqtt/topic-definition.html

        Most importantly, the arguments for the function is contained in json_payload['data']

    Returns:
        Dict: returns a dictionary object which will be sent back to the caller
    """

    # This function simply echos back whatever sent to this process
    return json_payload['data']

callee = Callee(
    host ="broker",
    port = 1883,
    username = "test",
    default_reply_topic = "/local/reply",
    default_request_topic="/local/request"
)

callee.register_call("echo", echo)
e = Event()
e.wait()    # wait indefinitely