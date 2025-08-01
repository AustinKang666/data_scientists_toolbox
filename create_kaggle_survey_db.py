

import pandas as pd  
import sqlite3


class CreateKaggleSurveyDB:
    """
    📝 資料清洗與資料庫建構主類別（2020~2022 Kaggle 問卷）

    - 處理每年度 CSV 問卷檔案，解析欄位 → 統一題號、題型、題目敘述
    - 整理出 questions（題目表）與 responses（回覆表）DataFrame 並寫入 SQLite 資料庫
    - 額外建立一個 view: `aggregated_responses`（彙總每題各選項的人數）

    最終產出表格結構如下：

    📄 questions（題目資訊）
        - question_index: 統一題號（如 Q7、Q26A）
        - question_type: 題型（Multiple choice / Multiple selection）
        - question_description: 題目描述
        - surveyed_year: 該題出現在問卷的哪一年

    📄 responses（回覆資訊）
        - respondent_id: 填答者代號
        - question_index: 統一題號（對應 questions）
        - response: 受訪者的填答內容
        - responded_in: 該筆回覆來自哪一年的問卷
    """


    def __init__(self):
        """
        🚀 初始化：讀取各年度問卷資料，並存入 df_dict 結構中
        """
        
        # 要載入的年份清單
        survey_years = [2020, 2021, 2022]

        # 儲存每年度資料的字典（以年份為 key）
        df_dict = dict()

        # 依序處理每一年度的問卷資料
        for year in survey_years:
            # 根據年度組成檔案路徑（例如 data/kaggle_survey_2020_responses.csv）
            file_path = f"data/kaggle_survey_{year}_responses.csv"

            # 使用函式載入回答與題目敘述
            responses, questions = self.load_kaggle_survey(file_path)

            # 將結果以年份為 key，儲存成一個 dict 包含兩份資料
            df_dict[year] = {
                "question_descriptions": questions,        # 題目敘述 (List)
                "responses": responses                     # 問卷填答內容 (df)
            }
        
        self.survey_years = survey_years
        self.df_dict = df_dict

        # (檢查用)
        # print(df_dict[2020]["question_descriptions"][:5])    # 觀察 題目敘述
        # print("-----------------")
        # column_names = df_dict[2020]["responses"].columns    # 觀察 欄位名稱
        # for elem in column_names:
        #     print(elem.split("_"))


    def load_kaggle_survey(self, file_path: str):
        """
        📥 載入指定年度的 CSV 檔案，回傳：
        - df_responses: 回覆資料（跳過第一列與第一欄）
        - question_descriptions: 題目描述（只取第一列並略過第一欄）
        """

        # 讀取問卷回答資料，略過 CSV 的第一列（skiprows=[1]）避免重複的題目列
        # low_memory=False 可避免 dtype 的警告
        df_responses = pd.read_csv(file_path, skiprows=[1], low_memory=False)
        df_responses = df_responses.iloc[:, 1:] # 移除第一行，只留下 欄位名稱 + 使用者回覆

        # 只讀取第一列（即原始題目欄位的問題）=> df
        question_row = pd.read_csv(file_path, nrows=1)

        # 從 question_row 資料中，取第 0 列、第 1 欄之後的所有欄位（略過時間欄位）
        # 並轉換為純 Python list 作為題目敘述欄位
        question_descriptions = question_row.iloc[0, 1:].tolist()

        # 回傳資料與對應題目敘述
        return df_responses, question_descriptions  


    def tidy_2020to2022_data(self, survey_year: int) -> tuple :
        """
        🧹 整理指定年度的題目與回覆資料：
        - 將欄位名稱轉成統一題號（如 Q7A、Q26）
        - 轉換寬格式回覆為長格式（每位填答者每題一筆資料）
        - 去除空值
        回傳：
        - question_df: 題目資訊表
        - response_df_melted: 回覆資訊表
        """

        # 從儲存結構中取得 欄位名稱 與 題目描述
        column_names = self.df_dict[survey_year]["responses"].columns         # e.g. ['Q1', 'Q2', ..., 'Q7_Part_1', ...]
        descriptions = self.df_dict[survey_year]["question_descriptions"]     # e.g. ['What is your gender? - Selected Choice', 'In which country do you currently reside?', 'What is the highest level of formal education that you have attained or plan to attain within the next 2 years?' ...]

        # 準備三個輸出列表
        question_indexes = []        # 題號（例如：Q7、Q7A）
        question_types = []          # 題型（Multiple choice / Multiple selection）
        question_descriptions = []   # 題目敘述（只保留前半句，"- Selected Choice' " 這段不取用）
        survey_years = []  # 用來儲存每個題目的調查年份，長度會與其他欄位（如 question_indexes）對齊


        # 定義 題號 與 描述 中的分隔符號
        QUESTION_ID_SEPARATOR = "_"
        DESCRIPTION_SEPARATOR = " - "

        # 對每一個 欄位名稱 與 題目描述 進行對應解析
        for column_name, description in zip(column_names, descriptions):

            # 將欄位名稱用 "_" 分割成各部分 → 方便判斷欄位結構（例如 Q26_A_Part_1 → ['Q26', 'A', 'Part', '1']）
            column_parts = column_name.split(QUESTION_ID_SEPARATOR)

            # 將題目描述用 " - " 拆開 → 分離主題與類型說明（例如 "Q1 - Selected Choice" → ['Q1', 'Selected Choice']）
            description_parts = description.split(DESCRIPTION_SEPARATOR)

            # ---------- 第一層判斷：欄位名稱是否為單一字串 ----------
            # e.g. Q1、Q2 → 表示單一題型（沒有子題或多欄位）
            if len(column_parts) == 1:
                question_index = column_parts[0]       # e.g. Q22
                question_type = "Multiple choice"      # 單選題型

            # ---------- 否則為多欄位命名（表示這題有子題、補充說明或是複選子欄位） 如 Q33_A_Part_2、Q7_OTHER、Q10_Part_3 等 ----------
            else:
                # 取出欄位名稱的前兩個部分
                prefix = column_parts[0]      # 主題題號（例如 Q23 或 Q26）
                suffix = column_parts[1]      # 切出的第二段內容，可能是 A / OTHER / Part

                # ---------- 第二層判斷：是否為子選項（如 A、B、C） ----------
                # 如果 suffix 是一個單一的大寫字母(區分如A <=> OTHER、Part)，表示這是多選子題的一部分（如 Q26_A_Part_1）
                if len(suffix) == 1 and suffix.isupper():
                    question_index = prefix + suffix   # 把 prefix 與 suffix 組合，當成題目索引（例如 Q26A）

                # ---------- 否則為補充欄位、子項說明或非標準格式 (如 OTHER、Part) ----------
                # 如：Q23_Part_1、Q23_OTHER → 把 Q23 當作主題即可
                else:
                    question_index = prefix

                # 所有多欄位的題目一律標為「Multiple selection」
                question_type = "Multiple selection"

            # ---------- 最後，把處理完的資訊加入結果清單 ----------
            question_indexes.append(question_index)                    # e.g. Q23、Q26A
            question_types.append(question_type)                       # Multiple choice / Multiple selection
            question_descriptions.append(description_parts[0])         # e.g. "What languages do you use?"
            survey_years.append(survey_year)                           # e.g. 2020、2021、2022



        # 將欄位名稱依序指定好（與資料順序對應）
        columns = ["question_index", "question_type", "question_description", "surveyed_year"]

        # 將每個欄位的 list 對應起來，形成一個 tuple
        question_data = (question_indexes, question_types, question_descriptions, survey_years)

        # [('question_index', [...]), ('question_type', [...]), ...] => 最後用 pd.DataFrame() 建立 DataFrame
        question_df = pd.DataFrame(dict(zip(columns, question_data)))

        # ➤ 依據四個欄位（問題代號、類型、描述、調查年份）做分組
        # ➤ 雖使用 .count()，但實際上所有欄位都已被 groupby，並無實質統計意義
        # ➤ count() 的作用在此僅是為了將 groupby 結果轉為 DataFrame，方便後續 reset_index()
        # ➤ reset_index() 將 groupby 的階層索引欄位轉回普通欄位，回到正常 DataFrame 結構
        question_df = question_df.groupby(["question_index", "question_type", "question_description", "surveyed_year"]).count().reset_index()


        # 從資料字典中取得指定年度的問卷回覆 (df)
        response_df = self.df_dict[survey_year]["responses"]

        # 將問卷的欄位名稱（原為 Q1、Q2_Part_1 等）改為前面整理過的 question_indexes（你前面依據欄位結構整理出的統一題號）
        # 這樣可確保同一題不同子項能對應到同一題號(確保 question_df、response_df 欄位名稱相同)，方便後續合併與統計
        response_df.columns = question_indexes

        # 將原本的 DataFrame 重設索引，把 index（受訪者編號）變成一個正式欄位
        # 因為後續 pd.melt() 會用這個 index（受訪者編號） 欄位來辨識不同的 respondent 
        # => 注意：這裡產生的欄位名稱會是 "index"，不是 "respondent_id"，稍後會重新命名成 "respondent_id"
        response_df_reset_index = response_df.reset_index()

        # (檢測用)
        # print(response_df_reset_index)


        # 使用 melt 函式將寬格式轉為長格式（每位受訪者每題一筆紀錄）
        # - id_vars: "index" 欄為識別欄（即 respondent_id => 後面會改名）
        # - var_name: 原本欄位名變成 "question_index" => Q1、Q2.....
        # - value_name: 回答內容欄名為 "response"
        response_df_melted = pd.melt(
            response_df_reset_index,
            id_vars="index",
            var_name="question_index",
            value_name="response"
        )

        # 加上一個欄位 "responded_in"，標示這筆回覆是來自哪一年份的問卷
        # 這在多年份合併分析時非常重要
        response_df_melted["responded_in"] = survey_year

        # 將 "index" 欄位重新命名為 "respondent_id"，讓資料語意更清楚（代表第幾位填答者）
        response_df_melted = response_df_melted.rename(columns={"index": "respondent_id"})

        # (檢測用)
        # print(response_df_melted)

        # 去除所有回覆為 NaN 的資料（即未填答的題目），保留有效填答紀錄 => "列"刪除
        # 並重新整理索引（避免 dropna 之後索引跳號）
        response_df_melted = response_df_melted.dropna().reset_index(drop=True)

        return question_df, response_df_melted


    def create_database(self):
        """
        🏗️ 整合三年資料並建立 SQLite 資料庫：
        - 資料表 questions、responses
        - 檢視表 aggregated_responses（彙總每題各選項的人數）
        """

        question_df = pd.DataFrame()  # 建立空的 DataFrame，用來儲存所有年份的題目資訊
        respnse_df = pd.DataFrame() # 建立空的 DataFrame，用來儲存所有年份的回覆資料

        # 依序處理每一個年度（2020、2021、2022）
        for survey_year in self.survey_years:
            q_df, r_df = self.tidy_2020to2022_data(survey_year)  # 呼叫前面整理好的 tidy 函式，取得指定年度的「題目資料」與「回覆資料」
            question_df = pd.concat([question_df, q_df], ignore_index=True)
            respnse_df = pd.concat([respnse_df, r_df], ignore_index=True)

        connection = sqlite3.connect("data/kaggle_survey.db")
        question_df.to_sql("questions", con=connection, index=False, if_exists="replace")  # 將整理好的題目資料寫入資料表 "questions"
        respnse_df.to_sql("responses", con=connection, index=False, if_exists="replace")  # 將整理好的回覆資料寫入資料表 "responses"

        # 準備建立新的檢視表
        cur = connection.cursor()

        # 若資料庫中已存在名為 aggregated_responses 的 view，先將其刪除
        drop_view_sql = """
        DROP VIEW IF EXISTS aggregated_responses;
        """

        # 建立新的 view (aggregated_responses)，用來彙總每一題各選項的回答人數
        # - JOIN：把 responses 與 questions 兩張表格透過「題號 + 年份」進行對應
        # - GROUP BY：以「年份 + 題號 + 回答內容」為單位進行分組
        # - COUNT：計算每一組有幾位 respondent 回答
        create_view_sql = """
        CREATE VIEW aggregated_responses AS
        SELECT questions.surveyed_year,
               questions.question_index,
               questions.question_type, 
               questions.question_description,
               responses.response,
               COUNT(responses.respondent_id) AS response_count
          FROM responses
          JOIN questions
            ON responses.question_index = questions.question_index AND
               responses.responded_in = questions.surveyed_year
         GROUP BY questions.surveyed_year,
                  questions.question_index,
                  responses.response; 
        """
        cur.execute(drop_view_sql)   # 執行刪除 view 的 SQL 指令（若有的話）
        cur.execute(create_view_sql)  # 執行建立 view 的 SQL 指令（產出彙總分析結果）
        connection.close()  # 關閉資料庫連線，確保所有變更寫入檔案


def test():
    create_kaggle_survey_db = CreateKaggleSurveyDB()
    create_kaggle_survey_db.create_database()


if __name__ == "__main__":
    test()