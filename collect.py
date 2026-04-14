import requests
import pandas as pd
from datetime import datetime
import os

API_KEY = os.environ.get("BUSAN_API_KEY")
URL = "http://apis.data.go.kr/6260000/BusanPblcPrkngInfoService/getPblcPrkngInfo"

GWANGAN_KEYWORDS = ["민락", "광안", "수영"]

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
        res = requests.get(URL, params=params, timeout=10)
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
