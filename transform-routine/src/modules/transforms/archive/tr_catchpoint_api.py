# Import modules
import csv
import json
import requests

# Parameters
parameters = {
    "client_id": "<Client ID>",
    "client_secret": "<Client Secret>",
    "grant_type": "client_credentials"
}

token_response = requests.post(url="https://io.catchpoint.com/ui/api/token",
                               data=parameters)
token = token_response.json()
access_token = token['access_token']
print(access_token)

auth = {"Authorization": "Bearer " + access_token}
data_response = requests.get(
    url="https://io.catchpoint.com/ui/api/v1/performance/favoriteCharts/64570/data",
    headers=auth)

data = json.loads(data_response.content)

header_list = []
metrics_dict_list = []

for i in data["summary"]["fields"]["synthetic_metrics"]:
    header_list.append(i["name"])
print(header_list)

for items in data["summary"]["items"]:
    metrics_dict = {}
    metrics_dict["Date"] = data["start"]
    metrics_dict["Test"] = items["breakdown_1"]["name"]
    for i in range(0, len(header_list)):
        metrics_dict[header_list[i]] = items["synthetic_metrics"][i]
    metrics_dict_list.append(metrics_dict)

header_list.insert(0, "Date")
header_list.insert(1, data["summary"]["fields"]["breakdown_1"]["name"])

with open('catchpoint.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=header_list)
    writer.writeheader()
    writer.writerows(metrics_dict_list)
