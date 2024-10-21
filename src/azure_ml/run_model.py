import os
import json

import onnxruntime as rt
import numpy as np



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
    # Parse JSON

    # Note: If you use the workaround where all feature are encoded as a single JSON string,
    # you need to add the following line:
    # raw_input = json.loads(raw_input)["jsonInput"]
    parsed_input = json.loads(raw_input)

    # Convert parsed input to dict with numpy arrays as values
    def convert_json_to_numpy_dict(data):
        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = np.array([[value]], dtype=object)
            elif value is None:
                result[key] = np.array([[np.nan]], dtype=np.float32)
            else:
                result[key] = np.array([[value]], dtype=np.float32)
        return result

    parsed_input = convert_json_to_numpy_dict(parsed_input)

    # Predict
    pred_onx = session.run(None, parsed_input)

    return pred_onx[0].tolist()

