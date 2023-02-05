from quixstreaming import StreamReader, StreamWriter, EventData
import random
import json
from datetime import datetime

class QuixFunction:
    def __init__(self, input_stream: StreamReader, output_stream: StreamWriter):
        self.input_stream = input_stream
        self.output_stream = output_stream


    def calc_meats(self):
        i = random.randint(0, 4)
        meats = ['pepperoni', 'sausage', 'ham', 'anchovies', 'salami', 'bacon']
        selection = []
        if i == 0:
            return 'none'
        else:
            for n in range(i):
                selection.append(meats[random.randint(0, 5)])

        return ' & '.join(set(selection))


    def add_meats(self, pizza):
        pizza['meats'] = self.calc_meats()


    # Callback triggered for each new event.
    def on_event_data_handler(self, data: EventData):
        print(data.value)

        # Here transform your data.
        pizza = json.loads(data.value)
        self.add_meats(pizza)
        new_data = EventData(event_id=pizza['order_id'], time=datetime.utcnow(), value=json.dumps(pizza))
        
        self.output_stream.events.write(new_data)




    