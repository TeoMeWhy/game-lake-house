import boto3
import json
import requests
import schedule
import time


def get_data(**kwargs):
    url = "https://api.opendota.com/api/live"
    data = requests.get(url, params=kwargs).json()
    return data


def filter_data(data):
    new_data = []
    for d in data:
        if d["league_id"] != 0:
            new_data.append(d)
    return new_data


def save_data_firehose(data, firehose):
    data = [[i] for i in data]

    d = json.dumps(data)[1:-1].replace("], [", "]\n[") + "\n"

    firehose.put_record(
        DeliveryStreamName="dota-live-games",
        Record={"Data": d},
    )


def execute(firehose):
    data = filter_data(get_data())
    save_data_firehose(data, firehose)


def main():
    firehose = boto3.client("firehose", region_name="us-east-1")
    schedule.every(1).minutes.do(execute, firehose)

    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == "__main__":
    main()