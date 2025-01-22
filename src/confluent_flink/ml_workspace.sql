/* Create model */
CREATE MODEL `frauddetection`
INPUT (
  TransactionID INT,
  TransactionDT FLOAT,
  TransactionAmt FLOAT,
  ProductCD STRING,
  card1 INT,
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
  V12 FLOAT,
  V13 FLOAT,
  V14 FLOAT,
  V15 FLOAT,
  V16 FLOAT,
  V17 FLOAT,
  V18 FLOAT,
  V19 FLOAT,
  V20 FLOAT,
  V21 FLOAT,
  V22 FLOAT,
  V23 FLOAT,
  V24 FLOAT,
  V25 FLOAT,
  V26 FLOAT,
  V27 FLOAT,
  V28 FLOAT,
  V29 FLOAT,
  V30 FLOAT,
  DT_D FLOAT,
  DT_W FLOAT,
  DT_M FLOAT,
  id_01 FLOAT,
  id_02 FLOAT,
  id_05 FLOAT,
  id_06 FLOAT,
  id_11 FLOAT,
  id_12 STRING,
  id_13 FLOAT,
  id_15 STRING,
  id_16 STRING,
  id_17 FLOAT,
  id_19 FLOAT,
  id_20 FLOAT,
  id_28 STRING,
  id_29 STRING,
  id_31 STRING,
  id_35 STRING,
  id_36 STRING,
  id_37 STRING,
  id_38 STRING,
  DeviceType STRING,
  DeviceInfo STRING
)
OUTPUT (isFraud INT) /* 0 or 1 */
WITH(
    'provider' = 'azureml',
    'task' = 'classification',
    'azureml.connection' = 'azureml-fraud-detection'
)

/* List and describe model */
SHOW MODELS;

DESCRIBE MODEL `frauddetection`;

/* Predict with model */
SELECT * FROM `transformed_records`, LATERAL TABLE(
ML_PREDICT(
      'frauddetection', /*model name*/
      TransactionID,
      TransactionDT,
      TransactionAmt,
      ProductCD,
      card1,
      card2,
      card3,
      card4,
      card5,
      card6,
      addr1,
      addr2,
      P_emaildomain,
      C1,
      C2,
      C3,
      C4,
      C5,
      C6,
      C7,
      C8,
      C9,
      C10,
      C11,
      C12,
      C13,
      C14,
      D1,
      D4,
      D10,
      D15,
      M6,
      V12,
      V13,
      V14,
      V15,
      V16,
      V17,
      V18,
      V19,
      V20,
      V21,
      V22,
      V23,
      V24,
      V25,
      V26,
      V27,
      V28,
      V29,
      V30,
      dayOfYear,
      dayOfWeek,
      `month`,
      id_01,
      id_02,
      id_05,
      id_06,
      id_11,
      id_12,
      id_13,
      id_15,
      id_16,
      id_17,
      id_19,
      id_20,
      id_28,
      id_29,
      id_31,
      id_35,
      id_36,
      id_37,
      id_38,
      DeviceType,
      DeviceInfo
    )
)


/* 
 * Workaround if you use more than 85 features: 
 * Flink crashes if you call ML_PREDICT with more than 85 features. 
 * Therefore, encode all input features into a single features in JSON format.
 */

/* 
 * Create model
 */
CREATE MODEL `frauddetection_json`
INPUT (jsonInput STRING)
OUTPUT (isFraud INT) /* 0 or 1 */
WITH(
    'provider' = 'azureml',
    'task' = 'classification',
    'azureml.connection' = 'azureml-fraud-detection'
)

/* Predict with model */
SELECT * FROM `transformed_records_json`, LATERAL TABLE(ML_PREDICT('frauddetection_json', InputJson))