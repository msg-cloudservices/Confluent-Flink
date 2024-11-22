import urllib.request
import json
import os
import ssl
import multiprocessing

def allowSelfSignedHttps(allowed):
    # Bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True)  # Allow self-signed certificates if needed

# Request data
data = {
    "input_data": {
        "data": [[
            '{"TransactionID":3357788.0,"TransactionDT":9228284.0,"TransactionAmt":10.392,"ProductCD":"C","card1":9633.0,"card2":130.0,"card3":185.0,"card4":"visa","card5":138.0,"card6":"debit","addr1":null,"addr2":null,"P_emaildomain":"icloud.com","C1":4.0,"C2":5.0,"C3":0.0,"C4":2.0,"C5":0.0,"C6":2.0,"C7":2.0,"C8":2.0,"C9":0.0,"C10":3.0,"C11":1.0,"C12":1.0,"C13":1.0,"C14":1.0,"D1":0.0,"D4":0.0,"D10":0.0,"D15":150.0,"M6":"","V12":0.0,"V13":0.0,"V14":1.0,"V15":1.0,"V16":1.0,"V17":1.0,"V18":1.0,"V19":1.0,"V20":1.0,"V21":1.0,"V22":1.0,"V23":1.0,"V24":1.0,"V25":1.0,"V26":1.0,"V27":0.0,"V28":0.0,"V29":0.0,"V30":0.0,"V31":1.0,"V32":1.0,"V33":1.0,"V34":1.0,"V35":0.0,"V36":0.0,"V37":2.0,"V38":2.0,"V39":1.0,"V40":1.0,"V41":1.0,"V42":2.0,"V43":2.0,"V44":2.0,"V45":2.0,"V46":1.0,"V47":1.0,"V48":0.0,"V49":0.0,"V50":1.0,"V51":1.0,"V52":1.0,"V53":0.0,"V54":0.0,"V55":2.0,"V56":2.0,"V57":2.0,"V58":2.0,"V59":2.0,"V60":2.0,"V61":2.0,"V62":2.0,"V63":2.0,"V64":2.0,"V65":1.0,"V66":1.0,"V67":1.0,"V68":0.0,"V69":0.0,"V70":0.0,"V71":2.0,"V72":2.0,"V73":1.0,"V74":1.0,"V75":0.0,"V76":0.0,"V77":2.0,"V78":2.0,"V79":1.0,"V80":1.0,"V81":1.0,"V82":1.0,"V83":1.0,"V84":1.0,"V85":1.0,"V86":2.0,"V87":2.0,"V88":1.0,"V89":0.0,"V90":0.0,"V91":0.0,"V92":1.0,"V93":1.0,"V94":1.0,"V95":0.0,"V96":0.0,"V97":0.0,"V98":0.0,"V99":0.0,"V100":0.0,"V101":0.0,"V102":0.0,"V103":0.0,"V104":0.0,"V105":0.0,"V106":0.0,"V107":1.0,"V108":1.0,"V109":1.0,"V110":1.0,"V111":1.0,"V112":1.0,"V113":1.0,"V114":1.0,"V115":1.0,"V116":1.0,"V117":1.0,"V118":1.0,"V119":1.0,"V120":1.0,"V121":1.0,"V122":1.0,"V123":1.0,"V124":1.0,"V125":1.0,"V126":0.0,"V127":0.0,"V128":0.0,"V129":0.0,"V130":0.0,"V131":0.0,"V132":0.0,"V133":0.0,"V134":0.0,"V135":0.0,"V136":0.0,"V137":0.0,"V279":1.0,"V280":1.0,"V281":1.0,"V282":2.0,"V283":2.0,"V284":1.0,"V285":1.0,"V286":0.0,"V287":1.0,"V288":1.0,"V289":1.0,"V290":2.0,"V291":2.0,"V292":2.0,"V293":0.0,"V294":0.0,"V295":0.0,"V296":0.0,"V297":0.0,"V298":0.0,"V299":0.0,"V300":0.0,"V301":0.0,"V302":2.0,"V303":2.0,"V304":2.0,"V305":1.0,"V306":42.7774,"V307":42.7774,"V308":42.7774,"V309":42.7774,"V310":42.7774,"V311":0.0,"V312":42.7774,"V313":42.7774,"V314":42.7774,"V315":42.7774,"V316":0.0,"V317":0.0,"V318":0.0,"V319":0.0,"V320":0.0,"V321":0.0,"DT_D":440.0,"DT_W":56.0,"DT_M":15.0,"id_01":-15.0,"id_02":84435.0,"id_05":-4.0,"id_06":-6.0,"id_11":100.0,"id_12":"NotFound","id_13":52.0,"id_15":"Found","id_16":"Found","id_17":225.0,"id_19":266.0,"id_20":325.0,"id_28":"Found","id_29":"Found","id_31":"chrome generic","id_35":"F","id_36":"F","id_37":"T","id_38":"F","DeviceType":"desktop","DeviceInfo":"Windows"}'
        ]]
    }
}

body = str.encode(json.dumps(data))

url = 'https://flinkaidemo-ubwnj.northeurope.inference.ml.azure.com/score'
api_key = os.environ.get("API_KEY")

headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}

# Define the request function
def make_request():
    while True:
        req = urllib.request.Request(url, body, headers)
        try:
            response = urllib.request.urlopen(req)
            result = response.read()
            print(result)
        except urllib.error.HTTPError as error:
            print("Request failed with status code:", error.code)
            print(error.info())
            print(error.read().decode("utf8", 'ignore'))

# Run the request function in parallel 10 times
if __name__ == "__main__":
    # Create a pool of processes
    with multiprocessing.Pool(10) as pool:
        # Run the make_request function 10 times in parallel
        pool.map(make_request(), range(10))

