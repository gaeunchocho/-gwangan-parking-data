import requests
import pandas as pd
from datetime import datetime
import os
import time

API_KEY = os.environ.get("BUSAN_API_KEY")
URL = "http://apis.data.go.kr/6260000/BusanPblcPrkngInfoService/getPblcPrkngInfo"

GWANGAN_KEYWORDS = ["민락", "광안", "수영"]

def fetch_with_retry(params, retries=3):
    for i in range(retries):
        try:
            res = requests.get(URL, params=params, timeout=30)
            return res
        except Exception as e:
            print(f"재시도 {i+1}/{retries}: {e}")
            time.sleep(5)
    raise Exception("API 호출 실패")

def collect():
    all_items = []
    page = 1

    while True:
        params = {
            "serviceKey": API_KEY,
            "numOfRows": 100,
            "pageNo": page,
            "resultType": "json"
        }
        res = fetch_with_retry(params)
        data = res.json()

        body = data["response"]["body"]
        items = body["items"]["item"]
        if isinstance(items, dict):
            items = [items]

        all_items.extend(items)

        total = int(body["totalCount"])
        if page * 100 >= total:
            break
        page += 1

    df = pd.DataFrame(all_items)

    mask = df["pkNam"].str.contains("|".join(GWANGAN_KEYWORDS), na=False)
    df_gwangan = df[mask].copy()
    df_gwangan["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{datetime.now()}] 수집된 주차장 수: {len(df_gwangan)}")
    print(df_gwangan[["pkNam", "currava", "pkCnt"]].to_string())

    output_path = "data/parking_log.csv"
    os.makedirs("data", exist_ok=True)
    header = not os.path.exists(output_path)
    df_gwangan.to_csv(output_path, mode="a", header=header, index=False)

if __name__ == "__main__":
    collect()
