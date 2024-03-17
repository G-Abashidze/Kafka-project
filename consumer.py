from confluent_kafka import Consumer, KafkaError
import time
import requests
import json
from pyodbc import connect

server = 'localhost'
database = 'kafkaProject'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

def import_orders(orderId, orderDetail):
    con = connect(conn_str, autocommit=True)
    cursor =  con.cursor()
    try:
        cursor.execute(
            f"""INSERT INTO orders (order_id, EventDate, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
                            VALUES(?, CAST(GETDATE() AS DATE), ?, ?, ?, ?, ?, ?, ?)
            """,
            orderId, orderDetail['customer_id'], orderDetail['order_status'], orderDetail['order_purchase_timestamp'], orderDetail['order_approved_at'],
            orderDetail['order_delivered_carrier_date'], orderDetail['order_delivered_customer_date'], orderDetail['order_estimated_delivery_date']
        )
    except Exception as e: 
        print(str(e))
        con.rollback()
    finally:
        cursor.close()
        con.close()


def consume_message():
    consume_message = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': '1',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consume_message)

    consumer.subscribe(['test_topic'])

    try:
        while 1:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"SOMETHING WRONG HAPPENED: {msg.error()}, VALUE={msg.value().decode('utf-8')}")
            
            value = json.loads(msg.value().decode('utf-8'))
            order_id = value['order_id']
            order_details = value['order_detail']
            order_items = value['order_items']
            print(value)
            import_orders(order_id, order_details)

            # print(f"HERE WILL BE API, NOW KEY={msg.key()}, VALUE={msg.value().decode('utf-8')}")
            
    except Exception as e: print(f'ERROR: {str(e)}')
    finally: consumer.close()


if __name__ == '__main__':
    consume_message()
