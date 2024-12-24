import pandas as pd
import requests
import logging
import os
from tqdm import tqdm
import time
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    filename="address_api_sync.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()

# API 호출 설정
API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"


# Load environment variables
load_dotenv()
CONFIRM_KEY = os.getenv("ADDRESS_API_KEY")
if not CONFIRM_KEY:
    raise ValueError(
        "API key not found. Please set ADDRESS_API_KEY in your .env file."
    ).strip()  # API 승인키

# 사용자 입력: folder_path 및 file_number
folder_path = input("Enter folder path (default: 202203): ").strip() or "202203"
file_number = input("Enter file number: ").strip()

file_path = f"./{folder_path}/{file_number}/{file_number}.csv"  # 입력된 파일 경로
output_folder = f"./{folder_path}/{file_number}"  # 동일한 폴더에 결과 저장


# API 호출 함수
def call_juso_api(address, retries=3, delay=3):
    params = {
        "currentPage": 1,
        "countPerPage": 1,
        "keyword": address,
        "confmKey": CONFIRM_KEY,
        "resultType": "json",
        "hstryYn": "Y",
    }

    for attempt in range(retries):
        try:
            response = requests.get(API_URL, params=params, timeout=50)
            if response.status_code == 200:
                data = response.json()
                if (
                    data["results"]["common"]["errorCode"] == "0"
                    and data["results"]["juso"]
                ):
                    juso = data["results"]["juso"][0]
                    return {
                        "시/도": juso.get("siNm", None),
                        "시/군/구": juso.get("sggNm", None),
                        "읍/면/동": juso.get("emdNm", None),
                    }
                else:
                    logger.error(
                        f"API Error: {data['results']['common']['errorMessage']} for address: {address}"
                    )
            else:
                logger.error(
                    f"HTTP Error: {response.status_code} for address: {address}"
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException: {e} for address: {address}")

        # 재시도 전 대기
        if attempt < retries - 1:
            logger.info(f"Retrying ({attempt + 1}/{retries}) for address: {address}")
            time.sleep(delay)

    logger.error(f"All retries failed for address: {address}")
    return None


# 데이터 처리 함수
def process_addresses(df):
    results = []
    error_records = []

    for idx, address in tqdm(
        df["n_addr"].items(), total=len(df), desc="Processing addresses"
    ):
        result = call_juso_api(address)
        if result is None:
            error_records.append(
                {
                    "Index": idx,
                    "주소": address,
                    "오류": "No result or error",
                }
            )
            results.append({"시/도": None, "시/군/구": None, "읍/면/동": None})
        else:
            results.append(result)

    return results, error_records


# 메인 함수
def main():
    # 데이터 읽기
    df = pd.read_csv(file_path, index_col=0, encoding="utf-8-sig")
    logger.info(f"Loaded data file: {file_path}")

    # 데이터 처리
    results, error_records = process_addresses(df)

    # 결과 저장: 동일한 폴더에 저장
    final_result_path = os.path.join(output_folder, f"final_address_{file_number}.csv")
    error_result_path = os.path.join(output_folder, f"error_address_{file_number}.csv")

    pd.DataFrame(results).to_csv(final_result_path, index=False)
    pd.DataFrame(error_records).to_csv(error_result_path, index=False)

    print(f"Processing complete for file {file_number}.")
    print(f"Results saved to: {final_result_path}")
    print(f"Errors saved to: {error_result_path}")


if __name__ == "__main__":
    main()
