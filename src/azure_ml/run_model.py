import os
import json
import logging
from typing import Dict

import onnxruntime as rt
import numpy as np


def check_type(key: str) -> str:
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

def convert_to_numpy_dict(data: Dict) -> Dict:
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
    model_name = "pipeline_75"
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
    parsed_input = json.loads(raw_input)
    print(parsed_input)
    feature_names = parsed_input["input_data"]["columns"]
    values = parsed_input["input_data"]["data"][0]
    
    # Convert parsed input to dict with numpy arrays as values
    input_data = convert_to_numpy_dict(dict(zip(feature_names, values)))

    # Predict
    pred_onx = session.run(None, input_data)

    return pred_onx[0].tolist()

