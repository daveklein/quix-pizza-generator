from quixstreaming import StreamReader, StreamWriter, EventData
import json
import random
from datetime import datetime

class QuixFunction:
    def __init__(self, input_stream: StreamReader, output_stream: StreamWriter):
        self.input_stream = input_stream
        self.output_stream = output_stream


    def calc_sauce(self):
        i = random.randint(0, 8)
        sauces = ['regular', 'light', 'extra', 'none', 'alfredo', 'regular', 'light', 'extra', 'alfredo']
        return sauces[i]


    def add_sauce(self, pizza):
        pizza['sauce'] = self.calc_sauce()
        

    # Callback triggered for each new event.
    def on_event_data_handler(self, data: EventData):
        print(data.value)

        # Here transform your data.
        pizza = json.loads(data.value)
        self.add_sauce(pizza)
        new_data = EventData(event_id=pizza['order_id'], time=datetime.utcnow(), value=json.dumps(pizza))
        
        self.output_stream.events.write(new_data)

