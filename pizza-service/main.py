import preinstall

from quixstreaming import QuixStreamingClient, StreamEndType, StreamReader, AutoOffsetReset
import os
from flask import Flask
import json
from pizza_service import PizzaService


# Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
client = QuixStreamingClient()

# Change consumer group to a different constant if you want to run model locally.
print("Opening input and output topics")

input_topic = client.open_input_topic(os.environ["input"], auto_offset_reset=AutoOffsetReset.Latest)
output_topic = client.open_output_topic(os.environ["output"])
# Create a new stream to output data
output_stream = output_topic.create_stream()
ps = PizzaService(output_stream=output_stream)


# Callback called for each incoming stream
def read_stream(input_stream: StreamReader):
    # React to new data received from input topic.
    input_stream.events.on_read += ps.on_event_data_handler

# Hook up events before initiating read to avoid losing out on any data
input_topic.on_stream_received += read_stream

input_topic.start_reading()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

app = Flask(__name__)


@app.route('/order/<count>', methods=['POST'])
def order_pizzas(count):
    order_id = ps.order_pizzas(int(count))
    return json.dumps({"order_id": order_id})


@app.route('/order/<order_id>', methods=['GET'])
def get_order(order_id):
    return ps.get_order(order_id)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
