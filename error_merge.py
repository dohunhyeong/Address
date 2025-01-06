import pandas as pd

# 파일 경로
folder_path = input("Enter folder path: ").strip()
file_number = input("Enter file number: ").strip()

final_path = f"./{folder_path}/{file_number}/final_address_{file_number}.csv"  # 기존 final_address.csv 경로
error_path = f"./{folder_path}/{file_number}/post_error_{file_number}/post_final_{file_number}.csv"  # 변환된 post_final_*.csv 경로


def main():
    # 데이터 로드
    final_df = pd.read_csv(final_path)  # 기존 데이터
    error_df = pd.read_csv(error_path)  # 변환된 post_final_*.csv 데이터

    # Index를 기준으로 병합 및 업데이트
    for _, row in error_df.iterrows():  # 변환된 데이터 순회
        idx = row["Index"]  # error_df의 Index 값 가져오기

        # final_df의 'index' 컬럼에서 해당 Index 값을 가진 행 찾기
        matching_rows = final_df[final_df["index"] == idx]
        if not matching_rows.empty:  # 일치하는 행이 있는 경우
            final_idx = matching_rows.index[0]  # 일치하는 행의 실제 인덱스 가져오기
            if pd.isna(
                final_df.loc[final_idx, "시/도"]
            ):  # 해당 행의 '시/도' 값이 비어 있으면 업데이트
                final_df.loc[final_idx, ["시/도", "시/군/구", "읍/면/동"]] = row[
                    ["시/도", "시/군/구", "읍/면/동"]
                ]

    # 업데이트된 final_address.csv 저장
    final_df.to_csv(final_path, index=False, encoding="utf-8-sig")
    print(f"병합 완료: final_address_{file_number}.csv 업데이트됨.")


if __name__ == "__main__":
    main()
