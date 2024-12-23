import pandas as pd
import asyncio
import aiohttp
from aiohttp import ClientError
from tqdm.asyncio import tqdm
import logging
import os

# 로깅 설정
logging.basicConfig(
    filename="address_api.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()

# API 호출 설정
API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"
CONFIRM_KEY = "U01TX0FVVEgyMDI0MTIwODEzMDQxMzExNTMwNzg="  # API 승인키

# 사용자 입력: folder_path 및 file_number
folder_path = input("Enter folder path (default: 202203): ").strip() or "202203"
file_number = input("Enter file number: ").strip()
file_path = f"./{folder_path}/{file_number}/{file_number}.csv"  # 입력된 파일 경로
output_folder = f"./{folder_path}/{file_number}"  # 동일한 폴더에 결과 저장

# API 호출 병렬 수 제한 설정
semaphore = asyncio.Semaphore(50)  # 동시 최대 50개의 API 요청

# API 호출 함수
async def call_juso_api(session, address, retries=3, delay=3):
    params = {
        "currentPage": 1,
        "countPerPage": 1,
        "keyword": address,
        "confmKey": CONFIRM_KEY,
        "resultType": "json",
        "hstryYn": "Y"
    }
    for attempt in range(retries):
        try:
            async with semaphore:  # 병렬 요청 수 제한 적용
                async with session.get(API_URL, params=params, timeout=50) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data["results"]["common"]["errorCode"] == "0" and data["results"]["juso"]:
                            juso = data["results"]["juso"][0]
                            return {
                                "시/도": juso.get("siNm", None),
                                "시/군/구": juso.get("sggNm", None),
                                "읍/면/동": juso.get("emdNm", None)
                            }
                    else:
                        logger.error(f"API Error: {response.status} for address: {address}")
        except ClientError as e:
            logger.error(f"ClientError: {e} for address: {address}")
        except asyncio.TimeoutError:
            logger.error(f"TimeoutError: Request timed out for address: {address}")
        except Exception as e:
            logger.error(f"Exception: {e} for address: {address}")
        await asyncio.sleep(delay)
    logger.error(f"All retries failed for address: {address}")
    return None

# 데이터 처리 함수
async def process_addresses(df):
    results = []
    error_records = []
    async with aiohttp.ClientSession() as session:
        tasks = [call_juso_api(session, address) for address in df["n_addr"]]
        for idx, response in enumerate(tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing addresses")):
            try:
                result = await response
                if result is None:
                    error_records.append({
                        "Index": df.index[idx],
                        "주소": df["n_addr"].iloc[idx],
                        "오류": "No result or error"
                    })
                    results.append({"시/도": None, "시/군/구": None, "읍/면/동": None})
                else:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error processing index {idx}: {e}")
                error_records.append({
                    "Index": df.index[idx],
                    "주소": df["n_addr"].iloc[idx],
                    "오류": str(e)
                })
                results.append({"시/도": None, "시/군/구": None, "읍/면/동": None})

    return results, error_records

# 메인 함수
async def main():
    # 데이터 읽기
    df = pd.read_csv(file_path, index_col=0)
    logger.info(f"Loaded data file: {file_path}")

    # 데이터 병렬 처리
    results, error_records = await process_addresses(df)

    # 결과 저장: 동일한 폴더에 저장
    final_result_path = os.path.join(output_folder, f"final_address_{file_number}.csv")
    error_result_path = os.path.join(output_folder, f"error_address_{file_number}.csv")

    pd.DataFrame(results).to_csv(final_result_path, index=False)
    pd.DataFrame(error_records).to_csv(error_result_path, index=False)

    print(f"Processing complete for file {file_number}.")
    print(f"Results saved to: {final_result_path}")
    print(f"Errors saved to: {error_result_path}")

if __name__ == "__main__":
    asyncio.run(main())
