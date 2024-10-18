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
    V31 FLOAT,
    V32 FLOAT,
    V33 FLOAT,
    V34 FLOAT,
    V35 FLOAT,
    V36 FLOAT,
    V37 FLOAT,
    V38 FLOAT,
    V39 FLOAT,
    V40 FLOAT,
    V41 FLOAT,
    V42 FLOAT,
    V43 FLOAT,
    V44 FLOAT,
    V45 FLOAT,
    V46 FLOAT,
    V47 FLOAT,
    V48 FLOAT,
    V49 FLOAT,
    V50 FLOAT,
    V51 FLOAT,
    V52 FLOAT,
    V53 FLOAT,
    V54 FLOAT,
    V55 FLOAT,
    V56 FLOAT,
    V57 FLOAT,
    V58 FLOAT,
    V59 FLOAT,
    V60 FLOAT,
    V61 FLOAT,
    V62 FLOAT,
    V63 FLOAT,
    V64 FLOAT,
    V65 FLOAT,
    V66 FLOAT,
    V67 FLOAT,
    V68 FLOAT,
    V69 FLOAT,
    V70 FLOAT,
    V71 FLOAT,
    V72 FLOAT,
    V73 FLOAT,
    V74 FLOAT,
    V75 FLOAT,
    V76 FLOAT,
    V77 FLOAT,
    V78 FLOAT,
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
SELECT * FROM `flink-streaming`.`flink-streamer`.`transformed_records`, LATERAL TABLE(ML_PREDICT('frauddetection', TransactionAmt, ProductCD, card1, card2, card3, card4, card5, card6, addr1, addr2, P_emaildomain, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, D1, D4, D10, D15, M6, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, V29, V30, V31, V32, V33, V34, V35, V36, V37, V38, V39, V40, V41, V42, V43, V44, V45, V46, V47, V48, V49, V50, V51, V52, V53, V54, V55, V56, V57, V58, V59, V60, V61, V62, V63, V64, V65, V66, V67, V68, V69, V70, V71, V72, V73, V74, V75, V76, V77, V78, DeviceType, DeviceInfo))