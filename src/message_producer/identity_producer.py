import csv
import os
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from identity import IdentityRecord


# Demo should run for ~60 mins if STREAM_WITH_TIME_GAPS = True.
# Otherwise producer will emit events as fast as possible.
STREAM_WITH_TIME_GAPS = True


def reading_to_dict(identity_event: IdentityRecord, ctx):
    return identity_event.to_dict()


def reading_key_to_dict(identity_key, ctx):
    return {"TransactionID":int(identity_key)}


def main():
    topic = 'identities'
    value_schema = 'src/message_producer/identity_schema.avsc'
    key_schema = 'src/message_producer/identity_key_schema.avsc'
    data_path = 'src/message_producer/data/identity.csv'

    cc_config = {
        'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVER"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get("IDENTITIES_USERNAME"),
        'sasl.password': os.environ.get("IDENTITIES_PASSWORD")
    }
   
    schema_registry_conf = {
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

    print("Producing identity records to topic {}. ^C to exit.".format(topic))

    with open(data_path, 'r') as f:
        next(f)
        previous_time = 0
        counter = 0
        reader = csv.reader(f, delimiter=',')
        for column in reader:   
            event = IdentityRecord(
                id_01=float(column[1]) if column[1] else None,
                id_02=float(column[2]) if column[2] else None,
                id_03=float(column[3]) if column[3] else None,
                id_04=float(column[4]) if column[4] else None,
                id_05=float(column[5]) if column[5] else None,
                id_06=float(column[6]) if column[6] else None,
                id_07=float(column[7]) if column[7] else None,
                id_08=float(column[8]) if column[8] else None,
                id_09=float(column[9]) if column[9] else None,
                id_10=float(column[10]) if column[10] else None,
                id_11=float(column[11]) if column[11] else None,
                id_12=column[12] if column[12] else "",
                id_13=float(column[13]) if column[13] else None,
                id_14=float(column[14]) if column[14] else None,
                id_15=column[15] if column[15] else "",
                id_16=column[16] if column[16] else "",
                id_17=float(column[17]) if column[17] else None,
                id_18=float(column[18]) if column[18] else None,
                id_19=float(column[19]) if column[19] else None,
                id_20=float(column[20]) if column[20] else None,
                id_21=float(column[21]) if column[21] else None,
                id_22=float(column[22]) if column[22] else None,
                id_23=column[23] if column[23] else "",
                id_24=float(column[24]) if column[24] else None,
                id_25=float(column[25]) if column[25] else None,
                id_26=float(column[26]) if column[26] else None,
                id_27=column[27] if column[27] else "",
                id_28=column[28] if column[28] else "",
                id_29=column[29] if column[29] else "",
                id_30=column[30] if column[30] else "",
                id_31=column[31] if column[31] else "",
                id_32=float(column[32]) if column[32] else None,
                id_33=column[33] if column[33] else "",
                id_34=column[34] if column[34] else "",
                id_35=column[35] if column[35] else "",
                id_36=column[36] if column[36] else "",
                id_37=column[37] if column[37] else "",
                id_38=column[38] if column[38] else "",
                DeviceType=column[39] if column[39] else "",
                DeviceInfo=column[40] if column[40] else "",
                TransactionDT=float(column[41]),
                Timestamp=(time.time())*1000
            )

            if STREAM_WITH_TIME_GAPS:
                time_delta = event.TransactionDT
                delay_in_s = (time_delta if (previous_time == 0) else time_delta - previous_time) 
                previous_time = time_delta
                time.sleep(delay_in_s)

            producer.produce(
                topic=topic,
                key=avro_key_serializer(column[0], SerializationContext(topic=topic, field=MessageField.KEY)),
                value=avro_serializer(event, SerializationContext(topic, MessageField.VALUE)),
            )

            # Periodic flush needed to prevent crashing producer due to full queue
            if counter%1000==0:
                producer.flush()
            counter += 1

    producer.flush()

main()