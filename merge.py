import pandas as pd
import os

# Base path
file_path = "./202201/"
file_list = os.listdir(file_path)
merged_list = []


def main():
    # Step 1: 기존 코드로 각 파일 병합
    for file_name in file_list:
        b_path = os.path.join(file_path, file_name)
        original_csv_name = f"{file_name}.csv"
        completed_csv_name = f"final_address_{file_name}.csv"
        main_name = os.path.join(b_path, original_csv_name)
        after_name = os.path.join(b_path, completed_csv_name)

        if os.path.exists(main_name) and os.path.exists(after_name):
            try:
                df1 = pd.read_csv(main_name, low_memory=False)
                df2 = pd.read_csv(after_name, index_col=0)
                df_final = pd.concat([df1, df2], axis=1)
                merged_csv_name = os.path.join(b_path, f"merged_{file_name}.csv")
                df_final.to_csv(merged_csv_name, index=False)
                merged_list.append(merged_csv_name)
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
        else:
            print(f"Files {main_name} or {after_name} do not exist.")

    # Step 2: 병합된 파일들 로드 및 행 기준으로 통합
    merged_dfs = []

    for merged_csv in merged_list:
        try:
            df = pd.read_csv(merged_csv, low_memory=False)
            merged_dfs.append(df)
        except Exception as e:
            print(f"Error reading {merged_csv}: {e}")

    # 모든 병합된 데이터프레임을 하나로 결합
    final_merged_df = pd.concat(merged_dfs, ignore_index=True)

    # Step 3: 엑셀 파일로 저장 (통합된 인덱스 사용)
    output_excel_path = os.path.join(file_path, "final_merged_output.xlsx")
    final_merged_df.to_excel(output_excel_path, index=False)

    print(f"Final merged Excel file saved at: {output_excel_path}")


if __name__ == "__main__":
    main()
