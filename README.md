# Fraud Detection with Confluent Flink
## Introduction
This repository contains the code for our project testing Confluent's integration of AI Model Inference on Confluent Cloud for Apache Flink. We used the Kaggle IEEE-CIS Fraud Detection Dataset and wrote an accompanying blog article providing hands-on instructions how to build this usecase yourself.
The link to the blog will shortly be published here.

## Running Event Producers
Set the following environment variables:
```Bash
export IDENTITIES_USERNAME="<identity-topic-key>"
export TRANSACTIONS_USERNAME="<transaction-topic-key>"
export IDENTITIES_PASSWORD="<identity-topic-secret>"
export TRANSACTIONS_PASSWORD="<transaction-topic-secret>"
export SR_URL="<schema-registery-url>"
export SR_USER_AUTH="<sr-key:sr-secret>"
export BOOTSTRAP_SERVER="<boostrap-server-address>"
```
Run the identity and transaction producers:
```Bash
python src/message_producer/transaction_producer.py & python src/message_producer/identity_producer.py
```

## Creating Flink connection to AzureML
Create a confluent connection to the AzureML endpoint:
"""
confluent login --save

confluent flink connection create "azureml-fraud-detection" --cloud "azure" --region "westeurope" --type "azureml" --endpoint "<endpoint-url>" --api-key "<api-key>" 
"""