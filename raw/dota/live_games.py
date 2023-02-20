import argparse
import boto3
import datetime
import dotenv
import json
import os
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
        if d == None:
            continue

        elif d["league_id"] != 0:
            new_data.append(d)

    return new_data


def save_data_firehose(data, firehose):
    data = [[i] for i in data]

    d = json.dumps(data)[1:-1].replace("], [", "]\n[") + "\n"

    firehose.put_record(
        DeliveryStreamName="dota-live-games",
        Record={"Data": d},
    )


def execute(firehose, api_key):
    print(datetime.datetime.now())
    data = get_data(api_key=api_key)
    filtered_data = filter_data(data)
    save_data_firehose(filtered_data, firehose)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interval",
        "-i",
        help="Intervalo em minutos para execução",
        default=5,
        type=int,
    )
    args = parser.parse_args()

    dotenv.load_dotenv(".env")
    api_key = os.getenv("API_KEY")

    firehose = boto3.client("firehose", region_name="us-east-1")
    schedule.every(args.interval).minutes.do(execute, firehose, api_key)

    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == "__main__":
    main()
