import json
from pizza import Pizza, PizzaOrder
from quixstreaming import StreamWriter, EventData
from datetime import datetime


class PizzaService:

    pizza_warmer = {}

    def __init__(self, output_stream: StreamWriter=None, pizza_warmer={}):
        self.output_stream = output_stream
        self.pizza_warmer = {}


    def order_pizzas(self, count):
        order = PizzaOrder(count)
        self.pizza_warmer[order.id] = order
        for i in range(count):
            new_pizza = Pizza()
            new_pizza.order_id = order.id
            data = EventData(event_id=order.id, time=datetime.utcnow(), value=new_pizza.toJSON())
            self.output_stream.events.write(data)

        return order.id

    def get_order(self, order_id):
        order = self.pizza_warmer[order_id]
        if order == None:
            return "Order not found, perhaps it's not ready yet."
        else:
            return order.toJSON()


    def add_pizza(self, order_id, pizza):
        if order_id in self.pizza_warmer.keys():
            order = self.pizza_warmer[order_id]
            order.add_pizza(pizza)


    def on_event_data_handler(self, data: EventData):
        print(data.value)
        # Add pizza to warming oven
        pizza = json.loads(data.value)
        self.add_pizza(pizza['order_id'], pizza)

