import pandas as pd
import os

# 원본 데이터 파일 경로
file_path = './202201.csv'

# 저장할 폴더 경로
output_folder = './202201'

# 배치 크기 설정
BATCH_SIZE = 10000

# 폴더 생성
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 데이터 분할 및 저장
df = pd.read_csv(file_path, index_col=0)
total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num, start in enumerate(range(0, len(df), BATCH_SIZE), start=1):
    batch_df = df.iloc[start:start + BATCH_SIZE]
    folder_path = os.path.join(output_folder, str(batch_num))
    os.makedirs(folder_path, exist_ok=True)  # 폴더 생성
    batch_df.to_csv(os.path.join(folder_path, f"{batch_num}.csv"), index=True)
    print(f"Saved batch {batch_num}.csv to {folder_path}")
