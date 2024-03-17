from typing import Union
from fastapi import FastAPI, HTTPException
import json
from pydantic import BaseModel
from confluent_kafka import Producer



app = FastAPI()


def delivery_report(err, msg):
	if err is not None:
		print(f"Message delivery failed: {err}")
	else:
		print(f"Message delivered to topic: {msg.topic()}; offset: {msg.offset()}; Value: {msg.value()} ")


producer_conf = {
    'bootstrap.servers': 'localhost'
}
producer = Producer(producer_conf)


class order_detail(BaseModel):
      customer_id: str
      order_status: str
      order_purchase_timestamp: str
      order_approved_at: str
      order_delivered_carrier_date: str
      order_delivered_customer_date: str
      order_estimated_delivery_date: str
class order_items(BaseModel):
      order_item_id: int
      product_id: str
      shipping_limit_date: str
      price: float
      freight_value: float  


class orders(BaseModel):
    order_id: str
    EventDate: str
    order_detail: order_detail
    order_items: order_items



@app.post("/orders/")
async def orders_post(orders: orders):
    try:
        producer.produce('test_topic', key=orders.order_id, value=orders.json(), callback=delivery_report)

        producer.poll(0)
        producer.flush()
        return {"message": f"{orders.order_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally: producer.flush()

    
    

if __name__ == '__main__':
    orders_post()