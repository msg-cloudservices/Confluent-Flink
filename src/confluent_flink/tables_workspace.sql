/* Create identity Table */
CREATE TABLE identities (
    TransactionID INT,
    id_01 FLOAT,
    id_02 FLOAT,
    id_03 FLOAT,
    id_04 FLOAT,
    id_05 FLOAT,
    id_06 FLOAT,
    id_07 FLOAT,
    id_08 FLOAT,
    id_09 FLOAT,
    id_10 FLOAT,
    id_11 FLOAT,
    id_12 STRING,
    id_13 FLOAT,
    id_14 FLOAT,
    id_15 STRING,
    id_16 STRING,
    id_17 FLOAT,
    id_18 FLOAT,
    id_19 FLOAT,
    id_20 FLOAT,
    id_21 FLOAT,
    id_22 FLOAT,
    id_23 STRING,
    id_24 FLOAT,
    id_25 FLOAT,
    id_26 FLOAT,
    id_27 STRING,
    id_28 STRING,
    id_29 STRING,
    id_30 STRING,
    id_31 STRING,
    id_32 FLOAT,
    id_33 STRING,
    id_34 STRING,
    id_35 STRING,
    id_36 STRING,
    id_37 STRING,
    id_38 STRING,
    DeviceType STRING,
    DeviceInfo STRING,
    TransactionDT FLOAT,
    TransactionDTDatetime STRING,
    `Timestamp` TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (TransactionID) NOT ENFORCED,
    WATERMARK FOR `Timestamp` AS `Timestamp` - INTERVAL '30' SECOND
) WITH ('changelog.mode' = 'append');

/* Create trasaction table */
CREATE TABLE transactions (
    TransactionID INT NOT NULL,
    TransactionDT FLOAT NOT NULL,
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
    dist1 FLOAT,
    dist2 STRING,
    P_emaildomain STRING,
    R_emaildomain STRING,
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
    D2 FLOAT,
    D3 FLOAT,
    D4 FLOAT,
    D5 FLOAT,
    D6 STRING,
    D7 STRING,
    D8 STRING,
    D9 STRING,
    D10 FLOAT,
    D11 FLOAT,
    D12 STRING,
    D13 STRING,
    D14 STRING,
    D15 FLOAT,
    M1 STRING,
    M2 STRING,
    M3 STRING,
    M4 STRING,
    M5 STRING,
    M6 STRING,
    M7 STRING,
    M8 STRING,
    M9 STRING,
    V1 FLOAT,
    V2 FLOAT,
    V3 FLOAT,
    V4 FLOAT,
    V5 FLOAT,
    V6 FLOAT,
    V7 FLOAT,
    V8 FLOAT,
    V9 FLOAT,
    V10 FLOAT,
    V11 FLOAT,
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
    V45  FLOAT,
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
    V79 FLOAT,
    V80 FLOAT,
    V81 FLOAT,
    V82 FLOAT,
    V83 FLOAT,
    V84 FLOAT,
    V85 FLOAT,
    V86 FLOAT,
    V87 FLOAT,
    V88 FLOAT,
    V89 FLOAT,
    V90 FLOAT,
    V91 FLOAT,
    V92 FLOAT,
    V93 FLOAT,
    V94 FLOAT,
    V95 FLOAT,
    V96 FLOAT,
    V97 FLOAT,
    V98 FLOAT,
    V99 FLOAT,
    V100 FLOAT,
    V101 FLOAT,
    V102 FLOAT,
    V103 FLOAT,
    V104 FLOAT,
    V105 FLOAT,
    V106 FLOAT,
    V107 FLOAT,
    V108 FLOAT,
    V109 FLOAT,
    V110 FLOAT,
    V111 FLOAT,
    V112 FLOAT,
    V113 FLOAT,
    V114 FLOAT,
    V115 FLOAT,
    V116 FLOAT,
    V117 FLOAT,
    V118 FLOAT,
    V119 FLOAT,
    V120 FLOAT,
    V121 FLOAT,
    V122 FLOAT,
    V123 FLOAT,
    V124 FLOAT,
    V125 FLOAT,
    V126 FLOAT,
    V127 FLOAT,
    V128 FLOAT,
    V129 FLOAT,
    V130 FLOAT,
    V131 FLOAT,
    V132 FLOAT,
    V133 FLOAT,
    V134 FLOAT,
    V135 FLOAT,
    V136 FLOAT,
    V137 FLOAT,
    V138 STRING,
    V139 STRING,
    V140 STRING,
    V141 STRING,
    V142 STRING,
    V143 STRING,
    V144 STRING,
    V145 STRING,
    V146 STRING,
    V147 STRING,
    V148 STRING,
    V149 STRING,
    V150 STRING,
    V151 STRING,
    V152 STRING,
    V153 STRING,
    V154 STRING,
    V155 STRING,
    V156 STRING,
    V157 STRING,
    V158 STRING,
    V159 STRING,
    V160 STRING,
    V161 STRING,
    V162 STRING,
    V163 STRING,
    V164 STRING,
    V165 STRING,
    V166 STRING,
    V167 STRING,
    V168 STRING,
    V169 STRING,
    V170 STRING,
    V171 STRING,
    V172 STRING,
    V173 STRING,
    V174 STRING,
    V175 STRING,
    V176 STRING,
    V177 STRING,
    V178 STRING,
    V179 STRING,
    V180 STRING,
    V181 STRING,
    V182 STRING,
    V183 STRING,
    V184 STRING,
    V185 STRING,
    V186 STRING,
    V187 STRING,
    V188 STRING,
    V189 STRING,
    V190 STRING,
    V191 STRING,
    V192 STRING,
    V193 STRING,
    V194 STRING,
    V195 STRING,
    V196 STRING,
    V197 STRING,
    V198 STRING,
    V199 STRING,
    V200 STRING,
    V201 STRING,
    V202 STRING,
    V203 STRING,
    V204 STRING,
    V205 STRING,
    V206 STRING,
    V207 STRING,
    V208 STRING,
    V209 STRING,
    V210 STRING,
    V211 STRING,
    V212 STRING,
    V213 STRING,
    V214 STRING,
    V215 STRING,
    V216 STRING,
    V217 STRING,
    V218 STRING,
    V219 STRING,
    V220 STRING,
    V221 STRING,
    V222 STRING,
    V223 STRING,
    V224 STRING,
    V225 STRING,
    V226 STRING,
    V227 STRING,
    V228 STRING,
    V229 STRING,
    V230 STRING,
    V231 STRING,
    V232 STRING,
    V233 STRING,
    V234 STRING,
    V235 STRING,
    V236 STRING,
    V237 STRING,
    V238 STRING,
    V239 STRING,
    V240 STRING,
    V241 STRING,
    V242 STRING,
    V243 STRING,
    V244 STRING,
    V245 STRING,
    V246 STRING,
    V247 STRING,
    V248 STRING,
    V249 STRING,
    V250 STRING,
    V251 STRING,
    V252 STRING,
    V253 STRING,
    V254 STRING,
    V255 STRING,
    V256 STRING,
    V257 STRING,
    V258 STRING,
    V259 STRING,
    V260 STRING,
    V261 STRING,
    V262 STRING,
    V263 STRING,
    V264 STRING,
    V265 STRING,
    V266 STRING,
    V267 STRING,
    V268 STRING,
    V269 STRING,
    V270 STRING,
    V271 STRING,
    V272 STRING,
    V273 STRING,
    V274 STRING,
    V275 STRING,
    V276 STRING,
    V277 STRING,
    V278 STRING,
    V279 FLOAT,
    V280 FLOAT,
    V281 FLOAT,
    V282 FLOAT,
    V283 FLOAT,
    V284 FLOAT,
    V285 FLOAT,
    V286 FLOAT,
    V287 FLOAT,
    V288 FLOAT,
    V289 FLOAT,
    V290 FLOAT,
    V291 FLOAT,
    V292 FLOAT,
    V293 FLOAT,
    V294 FLOAT,
    V295 FLOAT,
    V296 FLOAT,
    V297 FLOAT,
    V298 FLOAT,
    V299 FLOAT,
    V300 FLOAT,
    V301 FLOAT,
    V302 FLOAT,
    V303 FLOAT,
    V304 FLOAT,
    V305 FLOAT,
    V306 FLOAT,
    V307 FLOAT,
    V308 FLOAT,
    V309 FLOAT,
    V310 FLOAT,
    V311 FLOAT,
    V312 FLOAT,
    V313 FLOAT,
    V314 FLOAT,
    V315 FLOAT,
    V316 FLOAT,
    V317 FLOAT,
    V318 FLOAT,
    V319 FLOAT,
    V320 FLOAT,
    V321 FLOAT,
    V322 STRING,
    V323 STRING,
    V324 STRING,
    V325 STRING,
    V326 STRING,
    V327 STRING,
    V328 STRING,
    V329 STRING,
    V330 STRING,
    V331 STRING,
    V332 STRING,
    V333 STRING,
    V334 STRING,
    V335 STRING,
    V336 STRING,
    V337 STRING,
    V338 STRING,
    V339 STRING,  
    `Timestamp` TIMESTAMP(3) NOT NULL,
    TransactionDTDatetime STRING,
    PRIMARY KEY (TransactionID) NOT ENFORCED,
    WATERMARK FOR `Timestamp` AS `Timestamp` - INTERVAL '30' SECOND
) WITH ('changelog.mode' = 'append');

/* Join transaction and identity table */
CREATE TABLE `joined_records`AS 
SELECT * 
FROM `transactions` t
LEFT JOIN `identities` i 
ON t.TransactionID = i.TransactionID 
AND i.`Timestamp` BETWEEN t.`Timestamp` - INTERVAL '60' SECOND AND t.`Timestamp`+ INTERVAL '60' SECOND;

/* Add new time-based features */
CREATE TABLE `transformed_records` AS
WITH `transformations` AS (
    SELECT 
        event.*,
        TIMESTAMPADD(SECOND, CAST(`TransactionDT` AS INTEGER), TIMESTAMP '2017-11-30 00:00:00') AS `transactionDTDatetime`,      
        EXTRACT(DOY FROM TIMESTAMPADD(SECOND, CAST(`TransactionDT` AS INTEGER), TIMESTAMP '2017-11-30 00:00:00')) AS `dayOfYear`,
        EXTRACT(DOW FROM TIMESTAMPADD(SECOND, CAST(`TransactionDT` AS INTEGER), TIMESTAMP '2017-11-30 00:00:00')) AS `dayOfWeek`,
        EXTRACT(MONTH FROM TIMESTAMPADD(SECOND, CAST(`TransactionDT` AS INTEGER), TIMESTAMP '2017-11-30 00:00:00')) AS `month`
    FROM 
       `joined_records` AS event
)
SELECT * FROM `transformations`;

/* 
 * Encode all features as a JSON string 
 * (Only needed as a workaround if number of features for model prediction exceeds 84) 
 */
CREATE TABLE `transformed_records_json` AS
WITH `transformations_json` AS (
    SELECT JSON_OBJECT(
        'TransactionID' VALUE T.TransactionID,
        'TransactionDT' VALUE T.TransactionDT,
        'TransactionAmt' VALUE T.TransactionAmt,
        'ProductCD' VALUE T.ProductCD,
        'card1' VALUE T.card1,
        'card2' VALUE T.card2,
        'card3' VALUE T.card3,
        'card4' VALUE T.card4,
        'card5' VALUE T.card5,
        'card6' VALUE T.card6,
        'addr1' VALUE T.addr1,
        'addr2' VALUE T.addr2,
        'P_emaildomain' VALUE T.P_emaildomain,
        'C1' VALUE T.C1,
        'C2' VALUE T.C2,
        'C3' VALUE T.C3,
        'C4' VALUE T.C4,
        'C5' VALUE T.C5,
        'C6' VALUE T.C6,
        'C7' VALUE T.C7,
        'C8' VALUE T.C8,
        'C9' VALUE T.C9,
        'C10' VALUE T.C10,
        'C11' VALUE T.C11,
        'C12' VALUE T.C12,
        'C13' VALUE T.C13,
        'C14' VALUE T.C14,
        'D1' VALUE T.D1,
        'D4' VALUE T.D4,
        'D10' VALUE T.D10,
        'D15' VALUE T.D15,
        'M6' VALUE T.M6,
        'V12' VALUE T.V12,
        'V13' VALUE T.V13,
        'V14' VALUE T.V14,
        'V15' VALUE T.V15,
        'V16' VALUE T.V16,
        'V17' VALUE T.V17,
        'V18' VALUE T.V18,
        'V19' VALUE T.V19,
        'V20' VALUE T.V20,
        'V21' VALUE T.V21,
        'V22' VALUE T.V22,
        'V23' VALUE T.V23,
        'V24' VALUE T.V24,
        'V25' VALUE T.V25,
        'V26' VALUE T.V26,
        'V27' VALUE T.V27,
        'V28' VALUE T.V28,
        'V29' VALUE T.V29,
        'V30' VALUE T.V30,
        'V31' VALUE T.V31,
        'V32' VALUE T.V32,
        'V33' VALUE T.V33,
        'V34' VALUE T.V34,
        'V35' VALUE T.V35,
        'V36' VALUE T.V36,
        'V37' VALUE T.V37,
        'V38' VALUE T.V38,
        'V39' VALUE T.V39,
        'V40' VALUE T.V40,
        'V41' VALUE T.V41,
        'V42' VALUE T.V42,
        'V43' VALUE T.V43,
        'V44' VALUE T.V44,
        'V45' VALUE T.V45,
        'V46' VALUE T.V46,
        'V47' VALUE T.V47,
        'V48' VALUE T.V48,
        'V49' VALUE T.V49,
        'V50' VALUE T.V50,
        'V51' VALUE T.V51,
        'V52' VALUE T.V52,
        'V53' VALUE T.V53,
        'V54' VALUE T.V54,
        'V55' VALUE T.V55,
        'V56' VALUE T.V56,
        'V57' VALUE T.V57,
        'V58' VALUE T.V58,
        'V59' VALUE T.V59,
        'V60' VALUE T.V60,
        'V61' VALUE T.V61,
        'V62' VALUE T.V62,
        'V63' VALUE T.V63,
        'V64' VALUE T.V64,
        'V65' VALUE T.V65,
        'V66' VALUE T.V66,
        'V67' VALUE T.V67,
        'V68' VALUE T.V68,
        'V69' VALUE T.V69,
        'V70' VALUE T.V70,
        'V71' VALUE T.V71,
        'V72' VALUE T.V72,
        'V73' VALUE T.V73,
        'V74' VALUE T.V74,
        'V75' VALUE T.V75,
        'V76' VALUE T.V76,
        'V77' VALUE T.V77,
        'V78' VALUE T.V78,
        'V79' VALUE T.V79,
        'V80' VALUE T.V80,
        'V81' VALUE T.V81,
        'V82' VALUE T.V82,
        'V83' VALUE T.V83,
        'V84' VALUE T.V84,
        'V85' VALUE T.V85,
        'V86' VALUE T.V86,
        'V87' VALUE T.V87,
        'V88' VALUE T.V88,
        'V89' VALUE T.V89,
        'V90' VALUE T.V90,
        'V91' VALUE T.V91,
        'V92' VALUE T.V92,
        'V93' VALUE T.V93,
        'V94' VALUE T.V94,
        'V95' VALUE T.V95,
        'V96' VALUE T.V96,
        'V97' VALUE T.V97,
        'V98' VALUE T.V98,
        'V99' VALUE T.V99,
        'V100' VALUE T.V100,
        'V101' VALUE T.V101,
        'V102' VALUE T.V102,
        'V103' VALUE T.V103,
        'V104' VALUE T.V104,
        'V105' VALUE T.V105,
        'V106' VALUE T.V106,
        'V107' VALUE T.V107,
        'V108' VALUE T.V108,
        'V109' VALUE T.V109,
        'V110' VALUE T.V110,
        'V111' VALUE T.V111,
        'V112' VALUE T.V112,
        'V113' VALUE T.V113,
        'V114' VALUE T.V114,
        'V115' VALUE T.V115,
        'V116' VALUE T.V116,
        'V117' VALUE T.V117,
        'V118' VALUE T.V118,
        'V119' VALUE T.V119,
        'V120' VALUE T.V120,
        'V121' VALUE T.V121,
        'V122' VALUE T.V122,
        'V123' VALUE T.V123,
        'V124' VALUE T.V124,
        'V125' VALUE T.V125,
        'V126' VALUE T.V126,
        'V127' VALUE T.V127,
        'V128' VALUE T.V128,
        'V129' VALUE T.V129,
        'V130' VALUE T.V130,
        'V131' VALUE T.V131,
        'V132' VALUE T.V132,
        'V133' VALUE T.V133,
        'V134' VALUE T.V134,
        'V135' VALUE T.V135,
        'V136' VALUE T.V136,
        'V137' VALUE T.V137,
        'V279' VALUE T.V279,
        'V280' VALUE T.V280,
        'V281' VALUE T.V281,
        'V282' VALUE T.V282,
        'V283' VALUE T.V283,
        'V284' VALUE T.V284,
        'V285' VALUE T.V285,
        'V286' VALUE T.V286,
        'V287' VALUE T.V287,
        'V288' VALUE T.V288,
        'V289' VALUE T.V289,
        'V290' VALUE T.V290,
        'V291' VALUE T.V291,
        'V292' VALUE T.V292,
        'V293' VALUE T.V293,
        'V294' VALUE T.V294,
        'V295' VALUE T.V295,
        'V296' VALUE T.V296,
        'V297' VALUE T.V297,
        'V298' VALUE T.V298,
        'V299' VALUE T.V299,
        'V300' VALUE T.V300,
        'V301' VALUE T.V301,
        'V302' VALUE T.V302,
        'V303' VALUE T.V303,
        'V304' VALUE T.V304,
        'V305' VALUE T.V305,
        'V306' VALUE T.V306,
        'V307' VALUE T.V307,
        'V308' VALUE T.V308,
        'V309' VALUE T.V309,
        'V310' VALUE T.V310,
        'V311' VALUE T.V311,
        'V312' VALUE T.V312,
        'V313' VALUE T.V313,
        'V314' VALUE T.V314,
        'V315' VALUE T.V315,
        'V316' VALUE T.V316,
        'V317' VALUE T.V317,
        'V318' VALUE T.V318,
        'V319' VALUE T.V319,
        'V320' VALUE T.V320,
        'V321' VALUE T.V321,
        'DT_D' VALUE T.dayOfYear,
        'DT_W' VALUE T.dayOfWeek,
        'DT_M' VALUE T.`month`,
        'id_01' VALUE T.id_01,
        'id_02' VALUE T.id_02,
        'id_05' VALUE T.id_05,
        'id_06' VALUE T.id_06,
        'id_11' VALUE T.id_11,
        'id_12' VALUE T.id_12,
        'id_13' VALUE T.id_13,
        'id_15' VALUE T.id_15,
        'id_16' VALUE T.id_16,
        'id_17' VALUE T.id_17,
        'id_19' VALUE T.id_19,
        'id_20' VALUE T.id_20,
        'id_28' VALUE T.id_28,
        'id_29' VALUE T.id_29,
        'id_31' VALUE T.id_31,
        'id_35' VALUE T.id_35,
        'id_36' VALUE T.id_36,
        'id_37' VALUE T.id_37,
        'id_38' VALUE T.id_38,
        'DeviceType' VALUE T.DeviceType,
        'DeviceInfo' VALUE T.DeviceInfo
    ) as InputJson
    FROM `flink-streaming`.`flink-streamer`.`transformed_records` AS T
) SELECT * FROM `transformations_json`;