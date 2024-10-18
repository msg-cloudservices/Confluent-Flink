/* 
 * Create model
 * (Uses workaround where all features are encoded as a JSON string)
 */
CREATE MODEL `flink-streaming`.`flink-streamer`.`frauddetection_json`
INPUT (jsonInput STRING)
OUTPUT (isFraud INT) /* 0 or 1 */
WITH(
    'provider' = 'azureml',
    'task' = 'classification',
    'azureml.connection' = '<connection-name>'
)

/* Predict with model */
SELECT * FROM `flink-streaming`.`flink-streamer`.`transformed_records_json`, LATERAL TABLE(ML_PREDICT('frauddetection_json', InputJson))


/* Create model (with less than 85 features) */
CREATE MODEL `flink-streaming`.`flink-streamer`.`frauddetection`
INPUT (
    TransactionAmt FLOAT,
    ProductCD STRING,
    card1 FLOAT,
    card2 FLOAT,
    card3 FLOAT,
    card4 STRING,
    card5 FLOAT,
    card6 STRING,
    addr1 FLOAT,
    addr2 FLOAT,
    P_emaildomain STRING,
    C1 FLOAT,
    C2 FLOAT,
    C3 FLOAT,
    C4 FLOAT,
    C5 FLOAT,
    C6 FLOAT,
    C7 FLOAT,
    C8 FLOAT,
    C9 FLOAT,
    C10 FLOAT,
    C11 FLOAT,
    C12 FLOAT,
    C13 FLOAT,
    C14 FLOAT,
    D1 FLOAT,
    D4 FLOAT,
    D10 FLOAT,
    D15 FLOAT,
    M6 STRING,
    DeviceType STRING,
    DeviceInfo STRING
)
OUTPUT (isFraud INT) /* 0 or 1 */
WITH (
  'provider' = 'azureml',
  'task' = 'classification',
  'azureml.connection' = '<connection-name>'
)

/* Predict with model */
SELECT * FROM `flink-streaming`.`flink-streamer`.`transformed_records`, LATERAL TABLE(ML_PREDICT('frauddetection', TransactionAmt, ProductCD, card1, card2, card3, card4, card5, card6, addr1, addr2, P_emaildomain, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, D1, D4, D10, D15, M6, DeviceType, DeviceInfo))