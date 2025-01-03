import os
import json
import logging

import onnxruntime as rt
import numpy as np


def check_type(key) -> str:
    types = {
        # Tranaction types
        "TransactionDT": "float",
        "TransactionAmt": "float",
        "ProductCD": "str",
        "card1": "int",
        "card2": "float",
        "card3": "float",
        "card4": "str",
        "card5": "float",
        "card6": "str",
        "addr1": "float",
        "addr2": "float",
        "dist1": "float",
        "dist2": "float",
        "P_emaildomain": "str",
        "R_emaildomain": "str",
        "C1": "float",
        "C2": "float",
        "C3": "float",
        "C4": "float",
        "C5": "float",
        "C6": "float",
        "C7": "float",
        "C8": "float",
        "C9": "float",
        "C10": "float",
        "C11": "float",
        "C12": "float",
        "C13": "float",
        "C14": "float",
        "D1": "float",
        "D2": "float",
        "D3": "float",
        "D4": "float",
        "D5": "float",
        "D10": "float",
        "D11": "float",
        "D15": "float",
        "M1": "str",
        "M2": "str",
        "M3": "str",
        "M4": "str",
        "M5": "str",
        "M6": "str",
        "M7": "str",
        "M8": "str",
        "M9": "str",
        "V1": "float",
        "V2": "float",
        "V3": "float",
        "V4": "float",
        "V5": "float",
        "V6": "float",
        "V7": "float",
        "V8": "float",
        "V9": "float",
        "V10": "float",
        "V11": "float",
        "V12": "float",
        "V13": "float",
        "V14": "float",
        "V15": "float",
        "V16": "float",
        "V17": "float",
        "V18": "float",
        "V19": "float",
        "V20": "float",
        "V21": "float",
        "V22": "float",
        "V23": "float",
        "V24": "float",
        "V25": "float",
        "V26": "float",
        "V27": "float",
        "V28": "float",
        "V29": "float",
        "V30": "float",
        "V31": "float",
        "V32": "float",
        "V33": "float",
        "V34": "float",
        "V35": "float",
        "V36": "float",
        "V37": "float",
        "V38": "float",
        "V39": "float",
        "V40": "float",
        "V41": "float",
        "V42": "float",
        "V43": "float",
        "V44": "float",
        "V45": "float",
        "V46": "float",
        "V47": "float",
        "V48": "float",
        "V49": "float",
        "V50": "float",
        "V51": "float",
        "V52": "float",
        "V53": "float",
        "V54": "float",
        "V55": "float",
        "V56": "float",
        "V57": "float",
        "V58": "float",
        "V59": "float",
        "V60": "float",
        "V61": "float",
        "V62": "float",
        "V63": "float",
        "V64": "float",
        "V65": "float",
        "V66": "float",
        "V67": "float",
        "V68": "float",
        "V69": "float",
        "V70": "float",
        "V71": "float",
        "V72": "float",
        "V73": "float",
        "V74": "float",
        "V75": "float",
        "V76": "float",
        "V77": "float",
        "V78": "float",
        "V79": "float",
        "V80": "float",
        "V81": "float",
        "V82": "float",
        "V83": "float",
        "V84": "float",
        "V85": "float",
        "V86": "float",
        "V87": "float",
        "V88": "float",
        "V89": "float",
        "V90": "float",
        "V91": "float",
        "V92": "float",
        "V93": "float",
        "V94": "float",
        "V95": "float",
        "V96": "float",
        "V97": "float",
        "V98": "float",
        "V99": "float",
        "V100": "float",
        "V101": "float",
        "V102": "float",
        "V103": "float",
        "V104": "float",
        "V105": "float",
        "V106": "float",
        "V107": "float",
        "V108": "float",
        "V109": "float",
        "V110": "float",
        "V111": "float",
        "V112": "float",
        "V113": "float",
        "V114": "float",
        "V115": "float",
        "V116": "float",
        "V117": "float",
        "V118": "float",
        "V119": "float",
        "V120": "float",
        "V121": "float",
        "V122": "float",
        "V123": "float",
        "V124": "float",
        "V125": "float",
        "V126": "float",
        "V127": "float",
        "V128": "float",
        "V129": "float",
        "V130": "float",
        "V131": "float",
        "V132": "float",
        "V133": "float",
        "V134": "float",
        "V135": "float",
        "V136": "float",
        "V137": "float",
        "V138": "float",
        "V139": "float",
        "V140": "float",
        "V141": "float",
        "V142": "float",
        "V143": "float",
        "V144": "float",
        "V145": "float",
        "V146": "float",
        "V147": "float",
        "V148": "float",
        "V149": "float",
        "V150": "float",
        "V151": "float",
        "V152": "float",
        "V153": "float",
        "V154": "float",
        "V155": "float",
        "V156": "float",
        "V157": "float",
        "V158": "float",
        "V159": "float",
        "V160": "float",
        "V161": "float",
        "V162": "float",
        "V163": "float",
        "V164": "float",
        "V165": "float",
        "V166": "float",
        "V167": "float",
        "V168": "float",
        "V169": "float",
        "V170": "float",
        "V171": "float",
        "V172": "float",
        "V173": "float",
        "V174": "float",
        "V175": "float",
        "V176": "float",
        "V177": "float",
        "V178": "float",
        "V179": "float",
        "V180": "float",
        "V181": "float",
        "V182": "float",
        "V183": "float",
        "V184": "float",
        "V185": "float",
        "V186": "float",
        "V187": "float",
        "V188": "float",
        "V189": "float",
        "V190": "float",
        "V191": "float",
        "V192": "float",
        "V193": "float",
        "V194": "float",
        "V195": "float",
        "V196": "float",
        "V197": "float",
        "V198": "float",
        "V199": "float",
        "V200": "float",
        "V201": "float",
        "V202": "float",
        "V203": "float",
        "V204": "float",
        "V205": "float",
        "V206": "float",
        "V207": "float",
        "V208": "float",
        "V209": "float",
        "V210": "float",
        "V211": "float",
        "V212": "float",
        "V213": "float",
        "V214": "float",
        "V215": "float",
        "V216": "float",
        "V217": "float",
        "V218": "float",
        "V219": "float",
        "V220": "float",
        "V221": "float",
        "V222": "float",
        "V223": "float",
        "V224": "float",
        "V225": "float",
        "V226": "float",
        "V227": "float",
        "V228": "float",
        "V229": "float",
        "V230": "float",
        "V231": "float",
        "V232": "float",
        "V233": "float",
        "V234": "float",
        "V235": "float",
        "V236": "float",
        "V237": "float",
        "V238": "float",
        "V239": "float",
        "V240": "float",
        "V241": "float",
        "V242": "float",
        "V243": "float",
        "V244": "float",
        "V245": "float",
        "V246": "float",
        "V247": "float",
        "V248": "float",
        "V249": "float",
        "V250": "float",
        "V251": "float",
        "V252": "float",
        "V253": "float",
        "V254": "float",
        "V255": "float",
        "V256": "float",
        "V257": "float",
        "V258": "float",
        "V259": "float",
        "V260": "float",
        "V261": "float",
        "V262": "float",
        "V263": "float",
        "V264": "float",
        "V265": "float",
        "V266": "float",
        "V267": "float",
        "V268": "float",
        "V269": "float",
        "V270": "float",
        "V271": "float",
        "V272": "float",
        "V273": "float",
        "V274": "float",
        "V275": "float",
        "V276": "float",
        "V277": "float",
        "V278": "float",
        "V279": "float",
        "V280": "float",
        "V281": "float",
        "V282": "float",
        "V283": "float",
        "V284": "float",
        "V285": "float",
        "V286": "float",
        "V287": "float",
        "V288": "float",
        "V289": "float",
        "V290": "float",
        "V291": "float",
        "V292": "float",
        "V293": "float",
        "V294": "float",
        "V295": "float",
        "V296": "float",
        "V297": "float",
        "V298": "float",
        "V299": "float",
        "V300": "float",
        "V301": "float",
        "V302": "float",
        "V303": "float",
        "V304": "float",
        "V305": "float",
        "V306": "float",
        "V307": "float",
        "V308": "float",
        "V309": "float",
        "V310": "float",
        "V311": "float",
        "V312": "float",
        "V313": "float",
        "V314": "float",
        "V315": "float",
        "V316": "float",
        "V317": "float",
        "V318": "float",
        "V319": "float",
        "V320": "float",
        "V321": "float",
        "Timestamp": "long",
        # Identity types
        "id_01": "float",
        "id_02": "float",
        "id_03": "float",
        "id_04": "float",
        "id_05": "float",
        "id_06": "float",
        "id_07": "float",
        "id_08": "float",
        "id_09": "float",
        "id_10": "float",
        "id_11": "float",
        "id_12": "str",
        "id_13": "float",
        "id_14": "float",
        "id_15": "str",
        "id_16": "str",
        "id_17": "float",
        "id_18": "float",
        "id_19": "float",
        "id_20": "float",
        "id_21": "float",
        "id_22": "float",
        "id_23": "str",
        "id_24": "float",
        "id_25": "float",
        "id_26": "float",
        "id_27": "str",
        "id_28": "str",
        "id_29": "str",
        "id_30": "str",
        "id_31": "str",
        "id_32": "float",
        "id_33": "str",
        "id_34": "str",
        "id_35": "str",
        "id_36": "str",
        "id_37": "str",
        "id_38": "str",
        "DeviceType": "str",
        "DeviceInfo": "str"
    }

    return types[key]

def convert_json_to_numpy_dict(data):
    result = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = np.array([[value]], dtype=object)
        elif value is None:
            if check_type(key) == "str":
                result[key] = np.array([[""]], dtype=object)
            else:
                result[key] = np.array([[np.nan]], dtype=np.float32)
        else:
            result[key] = np.array([[value]], dtype=np.float32)
    return result


def init():
    model_name = "pipeline"
    # use AZUREML_MODEL_DIR to get your deployed model(s). If multiple models are deployed,
    # model_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), '$MODEL_NAME/$VERSION/$MODEL_FILE_NAME')
    model_dir = os.getenv('AZUREML_MODEL_DIR')
    model_path = os.path.join(model_dir, model_name + ".onnx")

    # Load model
    global session
    session = rt.InferenceSession(model_path, providers=["CPUExecutionProvider"])

def run(raw_input):
    logging.info(raw_input)

    # Parse JSON
    # Uses workaournd where all features are encoded in a JSON string.
    # Thus, we need to parse outer json first, extract the inner JSON string,
    # and then parse the inner JSON string.
    raw_input = json.loads(raw_input)["input_data"]["data"][0][0]
    parsed_input = json.loads(raw_input)
   
    # Convert parsed input to dict with numpy arrays as values
    parsed_input = convert_json_to_numpy_dict(parsed_input)

    # Predict
    pred_onx = session.run(None, parsed_input)

    return pred_onx[0].tolist()

