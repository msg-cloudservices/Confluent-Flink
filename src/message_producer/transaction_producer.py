import csv
import os
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from transaction import TransactionRecord


# Demo should run for ~60 mins if STREAM_WITH_TIME_GAPS = True.
# Otherwise producer will emit events as fast as possible.
STREAM_WITH_TIME_GAPS = False


def reading_to_dict(transaction_event: TransactionRecord, ctx):
    """
    This function converts the Transaction object to a dictionary which
    can be serialized into Avro format.
    """
    return transaction_event.to_dict()

def reading_key_to_dict(identity_key, ctx):
    return {"TransactionID":int(identity_key)}


def main():
    topic = 'transactions'
    value_schema = 'src/message_producer/transaction_schema.avsc'
    key_schema = 'src/message_producer/transaction_key_schema.avsc'
    data_path = 'src/message_producer/data/transaction.csv'

    cc_config = {
        'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVER"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get("TRANSACTIONS_USERNAME"),
        'sasl.password': os.environ.get("TRANSACTIONS_PASSWORD")
    }

    schema_registry_conf  = {
        'url': os.environ.get("SR_URL"),
        'basic.auth.user.info': os.environ.get("SR_USER_AUTH")
    }

    with open(f"{value_schema}") as f:
        schema_value_str = f.read()

    with open(f"{key_schema}") as f:
        schema_key_str = f.read()

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                    schema_value_str,
                                    reading_to_dict)

    avro_key_serializer = AvroSerializer(schema_registry_client,
                                    schema_key_str,
                                    reading_key_to_dict)

    producer = Producer(cc_config)

    print("Producing transaction records to topic {}. ^C to exit.".format(topic))

    with open(data_path, 'r') as f:
        previous_time = 0
        counter = 0
        next(f)
        reader = csv.reader(f, delimiter=',')
        for column in reader:   
            event = TransactionRecord(             
                TransactionID=int(column[0]),    
                TransactionDT=float(column[1]),
                TransactionAmt=float(column[2]),
                ProductCD=column[3] if column[3] else "",
                card1=int(column[4])if column[4] else None,
                card2=float(column[5]) if column[5] else None,
                card3=float(column[6]) if column[6] else None,
                card4=column[7] if column[7] else "",
                card5=float(column[8]) if column[8] else None,
                card6=column[9] if column[9] else "",
                addr1=float(column[10]) if column[10] else None,
                addr2=float(column[11]) if column[11] else None,
                dist1=float(column[12]) if column[12] else None,
                dist2=float(column[13]) if column[13] else None,
                P_emaildomain=column[14] if column[14] else "",
                R_emaildomain=column[15] if column[15] else "",
                C1=float(column[16]) if column[16] else None,
                C2=float(column[17]) if column[17] else None,
                C3=float(column[18]) if column[18] else None,
                C4=float(column[19]) if column[19] else None,
                C5=float(column[20]) if column[20] else None,
                C6=float(column[21]) if column[21] else None,
                C7=float(column[22]) if column[22] else None,
                C8=float(column[23]) if column[23] else None,
                C9=float(column[24]) if column[24] else None,
                C10=float(column[25]) if column[25] else None,
                C11=float(column[26]) if column[26] else None,                
                C12=float(column[27]) if column[27] else None,
                C13=float(column[28]) if column[28] else None,
                C14=float(column[29]) if column[29] else None,
                D1=float(column[30]) if column[30] else None,
                D2=float(column[31]) if column[31] else None,
                D3=float(column[32]) if column[32] else None,
                D4=float(column[33]) if column[33] else None,
                D5=float(column[34]) if column[34] else None,
                D10=float(column[39]) if column[39] else None,
                D11=float(column[40]) if column[40] else None,
                D15=float(column[44]) if column[44] else None,
                M1=column[45] if column[45] else "",
                M2=column[46] if column[46] else "",
                M3=column[47] if column[47] else "",
                M4=column[48] if column[48] else "",
                M5=column[49] if column[49] else "",
                M6=column[50] if column[50] else "",
                M7=column[51] if column[51] else "",
                M8=column[52] if column[52] else "",
                M9=column[53] if column[53] else "",
                **{f"V{i}": float(column[53 + i]) if column[53 + i] else None for i in range(1, 31)},
                Timestamp=(time.time())*1000,
                isFraud=(column[-1])
            )

            if STREAM_WITH_TIME_GAPS:
                time_delta = event.TransactionDT
                delay_in_s = (time_delta if (previous_time == 0) else time_delta - previous_time) 
                previous_time = time_delta
                time.sleep(delay_in_s)

            producer.produce(
                topic=topic,
                key=avro_key_serializer(column[0], SerializationContext(topic=topic, field=MessageField.KEY)),
                value=avro_serializer(event, SerializationContext(topic, MessageField.VALUE))
            )

            # Periodic flush needed to prevent crashing producer due to full queue
            if counter%1000==0:
                producer.flush()
            counter += 1

    producer.flush()

main()