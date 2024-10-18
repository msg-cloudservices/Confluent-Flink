from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import csv
import os
from dotenv import load_dotenv

from transaction import TransactionRecord

load_dotenv()

def reading_to_dict(transaction_event: TransactionRecord, ctx):
    """
    This function converts the Transaction object to a dictionary which
    can be serialized into Avro format.
    """
    return transaction_event.to_dict()

def main():
   topic = 'transaction'
   schema = 'transaction.avsc'
   data_path = "./transactions.csv"


   cc_config = {
       'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVER"),
       'security.protocol': 'SASL_SSL',
       'sasl.mechanisms': 'PLAIN',
       'sasl.username': '',
       'sasl.password': os.environ.get("TRANSACTIONS_PASSWORD")
   }

   sr_config = {
       'url': os.environ.get("URL"),
       'basic.auth.user.info': os.environ.get("USER_AUTH")
   }

   with open(f"{schema}") as f:
       schema_str = f.read()

   schema_registry_conf = sr_config
   schema_registry_client = SchemaRegistryClient(schema_registry_conf)

   avro_serializer = AvroSerializer(schema_registry_client,
                                    schema_str,
                                    reading_to_dict)
   string_serializer = StringSerializer('utf_8')

   producer = Producer(cc_config)

   print("Producing transaction records to topic {}. ^C to exit.".format(topic))


   with open(data_path, 'r') as f:
       previousTime = 0
       counter = 0
       next(f)
       reader = csv.reader(f, delimiter=',')
       for column in reader:   
            event = TransactionRecord(             
                TransactionID=int(column[0]),    
                TransactionDT=float(column[1]),
                TransactionAmt=float(column[2]),
                ProductCD=column[3],
                card1=int(column[4])if column[4] else None,
                card2=float(column[5]) if column[5] else None,
                card3=float(column[6]) if column[6] else None,
                card4=column[7] if column[7] else None,
                card5=float(column[8]) if column[8] else None,
                card6=column[9] if column[9] else None,
                addr1=float(column[10]) if column[10] else None,
                addr2=float(column[11]) if column[11] else None,
                dist1=float(column[12]) if column[12] else None,
                dist2=(column[13]) if column[13] else None,
                P_emaildomain=column[14] if column[14] else None,
                R_emaildomain=column[15] if column[15] else None,
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
                C11=float(column[26]) if column[26] else None ,                
                C12=float(column[27]) if column[27] else None,
                C13=float(column[28]) if column[28] else None,
                C14=float(column[29]) if column[29] else None,
                D1=float(column[30]) if column[30] else None,
                D2=float(column[31]) if column[31] else None,
                D3=float(column[32]) if column[32] else None,
                D4=float(column[33]) if column[33] else None,
                D5=float(column[34]) if column[34] else None,
                D6=(column[35]) if column[35] else None,
                D7=(column[36]) if column[36] else None,
                D8=(column[37]) if column[37] else None,
                D9=(column[38]) if column[38] else None,
                D10=float(column[39]) if column[39] else None,
                D11=float(column[40]) if column[40] else None,
                D12=(column[41]) if column[41] else None,
                D13=(column[42]) if column[42] else None,
                D14=(column[43]) if column[43] else None,
                D15=float(column[44]) if column[44] else None,
                M1=column[45] if column[45] else None,
                M2=column[46] if column[46] else None,
                M3=column[47] if column[47] else None,
                M4=column[48] if column[48] else None,
                M5=column[49] if column[49] else None,
                M6=column[50] if column[50] else None,
                M7=column[51] if column[51] else None,
                M8=column[52] if column[52] else None,
                M9=column[53] if column[53] else None,
                **{f"V{i}": column[53 + i] if i in range(138, 279) or i in range(322, 340) else float(column[53 + i]) if column[53 + i] else None for i in range(1, 340)},
                isFraud=(column[-1])
            )
            timeDelta = event.TransactionDT

            producer.produce(
                topic=topic,
                key=string_serializer(str(event.TransactionID), SerializationContext(topic=topic, field=MessageField.KEY)),
                value=avro_serializer(event, SerializationContext(topic, MessageField.VALUE))
            )
            counter += 1
            previousTime = timeDelta
            if counter%1000==0:
                producer.flush()

   producer.poll(10000)
   producer.flush()

main()