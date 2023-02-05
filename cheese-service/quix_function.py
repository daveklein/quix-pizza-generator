from quixstreaming import StreamReader, StreamWriter, EventData
import random
import json
from datetime import datetime

class QuixFunction:
    def __init__(self, input_stream: StreamReader, output_stream: StreamWriter):
        self.input_stream = input_stream
        self.output_stream = output_stream

    def calc_cheese(self):
        i = random.randint(0, 6)
        cheeses = ['extra', 'none', 'three cheese', 'goat cheese', 'extra', 'three cheese', 'goat cheese']
        return cheeses[i]

    def add_cheese(self, pizza):
        pizza['cheese'] = self.calc_cheese()


    # Callback triggered for each new event.
    def on_event_data_handler(self, data: EventData):
        print(data.value)

        # Here transform your data.
        pizza = json.loads(data.value)
        self.add_cheese(pizza)
        new_data = EventData(event_id=pizza['order_id'], time=datetime.utcnow(), value=json.dumps(pizza))

        self.output_stream.events.write(new_data)


