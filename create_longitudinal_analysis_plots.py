
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


def plot_horizontal_bars(sql_query: str, fig_name: str, survey_years=[2020, 2021, 2022], shareyaxis: bool = False):
    """
    從 SQLite 資料庫讀取資料，針對每一年度的回覆統計資料繪製橫向長條圖（每年一欄），
    並儲存成 PNG 圖檔。

    參數:
        sql_query (str): 要執行的 SQL 查詢語句
        fig_name (str): 儲存圖檔的檔名（不含副檔名）
        survey_years (list[int]): 要繪圖的年份列表
        shareyaxis (bool): 是否共用 y 軸（預設為 False）
    """

    # 1. 與 SQLite 建立連線並讀取 SQL 查詢結果
    with sqlite3.connect("data/kaggle_survey.db") as connection:
        response_counts = pd.read_sql(sql_query, con=connection)

    # 2. 建立繪圖畫布與子圖（每個年份各畫一張橫條圖）
    fig, axes = plt.subplots(
        ncols=len(survey_years),              # 橫向放置的圖表數量，等於年份數
        figsize=(11 * len(survey_years), 8),   # 整體圖像大小（寬度為每年 11 英吋，總寬隨年份變化，高度固定為 8 英吋）
        sharey=shareyaxis                     # 是否共用 y 軸（共用會使各年長條 y 軸類別一致）
    )

    # 3. 針對每一年份的資料繪製橫條圖
    for i, survey_year in enumerate(survey_years):
        # 取得該年度的回覆資料
        response_counts_year = response_counts[response_counts["surveyed_year"] == survey_year]

        # 取出 y 軸與長條寬度（x 軸）資料
        y = response_counts_year["response"].to_numpy()
        width = response_counts_year["response_count"].to_numpy()

        # 畫出橫向長條圖（Horizontal Bar Chart） => axes[i].barh(y軸標籤, 長條寬度)
        axes[i].barh(y, width)
        axes[i].set_title(f"{survey_year}")

    # 4. 自動排版避免重疊
    plt.tight_layout()

    # 5. 儲存圖檔
    fig.savefig(f"{fig_name}.png")



# ==============================================================================
# 📊 SQL 查詢區塊：針對每個主題（工作職稱、任務、技能等）建立對應查詢語句
# ==============================================================================

# sql_query_1: 查詢資料科學家的工作職稱（2020～2022） 
sql_query_1 = """
    SELECT surveyed_year,           -- 回覆的年份（2020、2021、2022）
           question_type,         -- 題目類型（Multiple choice / Multiple selection）
           response,              -- 每個選項的具體回答內容
           response_count         -- 每個選項被選擇的次數（統計數）
      FROM aggregated_responses   -- 從已整理好的 View 讀取資料（這個 View 是之前 create_database 時建立的）
     WHERE 
           (question_index = 'Q5' AND surveyed_year IN (2020, 2021))  -- 選出 Q5 題的 2020、2021 年資料
        OR 
           (question_index = 'Q23' AND surveyed_year = 2022)          -- 再選出 Q23 題的 2022 年資料（因為題目可能改名）
     ORDER BY 
           surveyed_year,         -- 先依照年份排序（2020 → 2021 → 2022）[升冪]
           response_count         -- 若年份相同，則依照同一年度中的 response_count [升冪] 進行排序
"""


# sql_query_2: 查詢資料科學家的工作任務（2020～2022）
sql_query_2 = """
SELECT surveyed_year,           -- 回覆年份（2020, 2021, 2022）
       question_type,         -- 題目類型（Multiple choice / Multiple selection）
       response,              -- 每個工作任務項目（例如：數據視覺化、建模等）
       response_count         -- 被選擇的次數（統計數）
  FROM aggregated_responses   -- 從整理好的 View 中查詢資料
 WHERE 
       (question_index = 'Q23' AND surveyed_year = 2020) OR    -- 2020年對應的題目代號
       (question_index = 'Q24' AND surveyed_year = 2021) OR    -- 2021年對應的題目代號
       (question_index = 'Q28' AND surveyed_year = 2022)       -- 2022年對應的題目代號
 ORDER BY 
       surveyed_year,            -- 先依年份排序
       response_count;         -- 年度內依照回答數排序
"""


# sql_query_3: 查詢從事資料科學工作者使用的程式語言（2020～2022）
sql_query_3 = """
SELECT surveyed_year, 
       question_type, 
       response, 
       response_count 
  FROM aggregated_responses 
 WHERE 
       (question_index = 'Q7' AND surveyed_year IN (2020, 2021)) OR   -- 2020、2021 年相同題目代碼
       (question_index = 'Q12' AND surveyed_year = 2022)              -- 2022 年題目代碼可能更動
 ORDER BY 
       surveyed_year, 
       response_count;
"""


# sql_query_4: 查詢資料科學家使用的資料庫系統（2020～2022）
sql_query_4 = """
SELECT surveyed_year, 
       question_type, 
       response, 
       response_count 
  FROM aggregated_responses 
 WHERE 
       (question_index = 'Q29A' AND surveyed_year = 2020) OR    -- 2020 年題目代碼
       (question_index = 'Q32A' AND surveyed_year = 2021) OR    -- 2021 年題目代碼
       (question_index = 'Q35' AND surveyed_year = 2022)        -- 2022 年題目代碼
 ORDER BY 
       surveyed_year, 
       response_count;
"""


# sql_query_5: 查詢資料科學家使用的機器學習工具（2020～2022）
sql_query_5 = """
SELECT surveyed_year, 
       question_type, 
       response, 
       response_count 
  FROM aggregated_responses 
 WHERE 
       (question_index = 'Q17' AND surveyed_year IN (2020, 2021)) OR  -- 2020、2021 年題號
       (question_index = 'Q18' AND surveyed_year = 2022)              -- 2022 年題號
 ORDER BY 
       surveyed_year, 
       response_count;
"""


# ==============================================================================
# 📈 執行繪圖區塊：根據 SQL 查詢結果產出橫條圖（每年一欄）
# ==============================================================================
plot_horizontal_bars(sql_query_1, "data_science_job_titles")
plot_horizontal_bars(sql_query_2, "data_science_job_tasks", shareyaxis=True)  # 因為三年都使用「相同的 y 軸選項順序」，因此設定 shareyaxis=True
plot_horizontal_bars(sql_query_3, "data_science_job_programming_languages")
plot_horizontal_bars(sql_query_4, "data_science_job_databases")
plot_horizontal_bars(sql_query_5, "data_science_job_machine_learnings")

