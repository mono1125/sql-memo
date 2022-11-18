# %%
import pandas as pd
import psycopg2
from psycopg2 import Error

# %%
df = pd.read_csv("data.csv", header=None)
# %%
new_column = []
for i in range(1, len(df.columns)+1):
    new_column.append(f'wf_{i}')
print(new_column)
df.columns = new_column
df.head(3)
# %%
# column_name列を追加, Falseで初期化
df["column_name"] = False
# 1行目はカラム名なので column_name をTrueにする
df.loc[0,"column_name"] = True
# %%
# ダブルクォーテーションを削除
df = df.replace(regex={r'^\s*\"': '', r'\"$': ''})
# シングルクォーテーションを削除
df = df.replace(regex={r'^\s*\'': '', r'\'$': ''})
df.head(3)
# %%
# NaNを空文字置換
# 空文字とNULLは別
df.fillna('', inplace=True)
# %%
# pandasのデータフレームを使用したfor文において脱iterrows()を試みたら実行時間が約70倍高速化した話
# https://qiita.com/daikikatsuragawa/items/1658134af600be2c1c16
for row in df.to_dict(orient="records"):
    print("row: ", row)
    print("wf_1: ", row["wf_1"])
# %%
df
# %%
# SQLのマージ(MERGE)文を使ってみよう！INSERTまたはUPDATEを実行する
# https://style.potepan.com/articles/25767.html
# SQL:レコードがない場合のみinsertしたい
# https://qiita.com/tsunenorikan/items/887007d825c54333201f
# DBごとのSQLのクォーテーションを整理したった
# https://qiita.com/Ping/items/d5d8468dadd9c1287f5e
try:
    connector = psycopg2.connect(
        "postgresql://{user}:{password}:@{host}:{port}/{dbname}".format(
            user="test_user", password="", host="localhost", port="5432", dbname="test"
        )
    )

    cursor = connector.cursor()
    cursor.execute("SELECT version();")
    result = cursor.fetchone()
    print(result[0] + "に接続しています")

    # トランザクション開始
    print("トランザクションを開始します")
    cursor.execute("BEGIN TRANSACTION;")
    # 1行ずつデータフレームを処理
    for row in df.to_dict(orient="records"):
        sql = f"""
        MERGE INTO insert_test_table as org
        /* 下記の条件のデータが存在するかを確認し、
        存在する場合には WHEN MATCHED THEN に指定したUPDATE文が実行され
        存在しない場合には WHEN NOT MATCHED THEN に指定した INSERT文が実行される
        */
        USING(
            VALUES ('{row["wf_1"]}', '{row["wf_2"]}', '{row["wf_3"]}', '{row["wf_4"]}', '{row["wf_5"]}', '{row["wf_6"]}', '{row["wf_7"]}', '{row["wf_8"]}', '{row["wf_9"]}', '{row["wf_10"]}', '{row["wf_11"]}', '{row["wf_12"]}', '{row["wf_13"]}', '{row["wf_14"]}', '{row["wf_15"]}', '{row["wf_16"]}', {row["column_name"]})
        ) as new (wf_1, wf_2, wf_3, wf_4, wf_5, wf_6, wf_7, wf_8, wf_9, wf_10, wf_11, wf_12, wf_13, wf_14, wf_15, wf_16, column_name)
        ON (org.wf_1 = new.wf_1)
        WHEN MATCHED THEN
            UPDATE SET
            time_stamp = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (wf_1, wf_2, wf_3, wf_4, wf_5, wf_6, wf_7, wf_8, wf_9, wf_10, wf_11, wf_12, wf_13, wf_14, wf_15, wf_16, column_name)
            VALUES (wf_1, wf_2, wf_3, wf_4, wf_5, wf_6, wf_7, wf_8, wf_9, wf_10, wf_11, wf_12, wf_13, wf_14, wf_15, wf_16, column_name);
        """
        print(sql)
        cursor.execute(sql)
    cursor.execute("COMMIT;")
    print("トランザクションをコミットしました")

except (Exception, Error) as error:
    print("PostgreSQLへの接続時にエラーが発生しました", error)
finally:
    cursor.close()
    connector.close()

# %%
