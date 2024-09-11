from flask import Flask, jsonify, request
import pyodbc

app = Flask(__name__)

def execute_sql_query(query, parameters=None):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; '
                              'SERVER=DESKTOP-RHICGL1\\FINPAL_DATA_WEB; '
                              'DATABASE=POWER_DATAMART; '
                              'UID=admin_finpal; '
                              'PWD=Pass@123')

        # Create a cursor
        cursor = conn.cursor()

        # Execute the query with parameters if provided
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)

        # Fetch the results
        result = cursor.fetchall()

        # Convert pyodbc.Row objects to dictionaries
        rows_as_dicts = [dict(zip([column[0] for column in cursor.description], row)) for row in result]

        return rows_as_dicts

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()

        if 'conn' in locals():
            conn.close()

@app.route('/api/data', methods=['GET'])
def get_data():
    # Get the value of PR_Ticker, PR_Date, DATA_TYPE, ITEM_VERTICAL, and FIELD_HORIZONTAL from the query parameters
    pr_ticker_value = request.args.get('PR_Ticker')
    pr_date_value = request.args.get('PR_Date')
    data_type_value = request.args.get('DATA_TYPE')
    item_vertical_value = request.args.get('ITEM_VERTICAL', None)
    field_horizontal_value = request.args.get('FIELD_HORIZONTAL', None)
    group_fs_value = request.args.get('GROUP2', None)

    # Set default values for PR_Ticker
    pr_ticker_default = 'AAA'
    data_type_default = 'D'

    # Use user-provided values if available; otherwise, use default values
    pr_ticker_value = pr_ticker_value if pr_ticker_value else pr_ticker_default
    data_type_value = data_type_value if data_type_value and data_type_value != 'D' else data_type_default

    # Build the WHERE clause based on the provided parameters
    where_clause_common = "PR_Ticker = ?"
    where_clause_ta_ft = "PR_Ticker = ? AND DATA_TYPE = ?"
    where_clause_fa_fs = "Ticker = ?"
    where_clause_ta_rrg = "Ticker = ?"
    parameters_common = [pr_ticker_value]
    parameters_ta_ft = [pr_ticker_value, data_type_value]
    parameters_fa_fs = [pr_ticker_value]

    if pr_date_value:
        pr_date_range = pr_date_value.replace(' TO ', ' AND ')
        where_clause_ta_ft += " AND PR_Date BETWEEN ? AND ?"
        parameters_ta_ft.extend(pr_date_range.split(' AND '))

    if item_vertical_value:
        where_clause_fa_fs += " AND ITEM_VERTICAL = ?"
        parameters_fa_fs.append(item_vertical_value)

    if field_horizontal_value:
        where_clause_fa_fs += " AND FIELD_HORIZONTAL = ?"
        parameters_fa_fs.append(field_horizontal_value)

    if group_fs_value:
        where_clause_fa_fs += " AND GROUP2 = ?"
        parameters_fa_fs.append(group_fs_value)

    # Execute queries for each table
    query_ta_sta = f"""
    WITH TA_FT AS
(
SELECT * FROM POWER_DATAMART.DBO.TA_FT_TEMP WHERE PR_DATE = (SELECT MAX(PR_DATE) FROM POWER_DATAMART.DBO.TA_FT_TEMP WHERE PR_TICKER = 'VNINDEX' AND DATA_TYPE = 'D') AND DATA_TYPE = 'D'
),
TA_FT_2 AS
(
SELECT DISTINCT PR_TICKER, MAX(TRY_CAST(PR_RENKO_COLOR AS FLOAT)) RENKO_SIGNAL , SUM(TRY_CAST(TRANS_NET_BUY_NN AS FLOAT)) TRANS_NET_BUY_NN, SUM(TRANS_NET_BUY_CTCK) TRANS_NET_BUY_CTCK
FROM POWER_DATAMART.DBO.TA_FT_TEMP 
WHERE DATA_TYPE = 'D' 
AND PR_DATE > GETDATE() - 7 
--AND PR_TICKER = 'BMP'
GROUP BY PR_TICKER
),
TA_FT_3 AS
(
SELECT DISTINCT PR_TICKER, MAX(TRY_CAST(PR_RENKO_COLOR AS FLOAT)) RENKO_SIGNAL , SUM(TRY_CAST(TRANS_NET_BUY_NN AS FLOAT)) TRANS_NET_BUY_NN, SUM(TRANS_NET_BUY_CTCK) TRANS_NET_BUY_CTCK
FROM POWER_DATAMART.DBO.TA_FT_TEMP 
WHERE DATA_TYPE = 'D' 
AND PR_DATE > GETDATE() - 30 
--AND PR_TICKER = 'BMP'
GROUP BY PR_TICKER
),
VALUATION_DETAIL AS
(
SELECT TICKER, [Phương pháp] METHOD, TRY_CAST([Giá trung bình] AS FLOAT) VALUE_PRICE FROM FINANCE_DATA.DBO.Valuation_Overall
)
,
FA_FS AS
(
SELECT
    TICKER,
    MAP_PERIOD_DATE,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN CONCAT(YEAR(MAP_PERIOD_DATE), ' Q', (MONTH(MAP_PERIOD_DATE) - 1) / 3 + 1)
        ELSE CAST(YEAR(MAP_PERIOD_DATE) AS VARCHAR(4))
    END AS MAP_PERIOD_STR,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN 'Q'
        ELSE 'Y'
    END AS PERIOD_TYPE,
	MAX(CASE WHEN ITEM_VERTICAL = N'Lợi nhuận sau thuế thu nhập doanh nghiệp' THEN VALUE END) AS [NET_PROFIT],
    MAX(CASE WHEN ITEM_VERTICAL = N'Biên lợi nhuận gộp' THEN VALUE END) AS [GROSS_PROFIT_MG],
    MAX(CASE WHEN ITEM_VERTICAL = N'Tăng trưởng LNST' THEN VALUE END) AS [NET_PROFIT_GROWTH],
	MAX(CASE WHEN ITEM_VERTICAL = N'Doanh thu' THEN VALUE END) AS [REVENUE],
    MAX(CASE WHEN ITEM_VERTICAL = N'Tăng trưởng doanh thu' THEN VALUE END) AS [REVENUE_GROWTH]
FROM 
    POWER_DATAMART.DBO.FA_FS_TEMP A
WHERE 
    GROUP2 IN ('FS_PL_Q', 'FS_GR_Q')
    AND ITEM_VERTICAL IN (
        N'Lợi nhuận sau thuế thu nhập doanh nghiệp', 
        N'Biên lợi nhuận gộp', 
        N'Tăng trưởng LNST', 
		N'Doanh thu', 
        N'Tăng trưởng doanh thu'
    )
    AND MAP_PERIOD_DATE = (SELECT MAX(MAP_PERIOD_DATE) FROM POWER_DATAMART.DBO.FA_FS_TEMP WHERE GROUP2 = 'FS_PL_Q' AND ITEM_VERTICAL = N'Lợi nhuận sau thuế thu nhập doanh nghiệp')
	--AND TICKER = 'TCB'
GROUP BY
    TICKER,
    MAP_PERIOD_DATE,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN CONCAT(YEAR(MAP_PERIOD_DATE), ' Q', (MONTH(MAP_PERIOD_DATE) - 1) / 3 + 1)
        ELSE CAST(YEAR(MAP_PERIOD_DATE) AS VARCHAR(4))
    END,
	CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN 'Q'
        ELSE 'Y'
    END 
--ORDER BY PERIOD_TYPE, MAP_PERIOD_STR ASC
)
,
FA_FS_Y AS
(
SELECT
    TICKER,
    MAP_PERIOD_DATE,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN CONCAT(YEAR(MAP_PERIOD_DATE), ' Q', (MONTH(MAP_PERIOD_DATE) - 1) / 3 + 1)
        ELSE CAST(YEAR(MAP_PERIOD_DATE) AS VARCHAR(4))
    END AS MAP_PERIOD_STR,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN 'Q'
        ELSE 'Y'
    END AS PERIOD_TYPE,
	MAX(CASE WHEN ITEM_VERTICAL = N'Lợi nhuận sau thuế thu nhập doanh nghiệp' THEN VALUE END) AS [NET_PROFIT],
    MAX(CASE WHEN ITEM_VERTICAL = N'Biên lợi nhuận gộp' THEN VALUE END) AS [GROSS_PROFIT_MG],
    MAX(CASE WHEN ITEM_VERTICAL = N'Tăng trưởng LNST' THEN VALUE END) AS [NET_PROFIT_GROWTH],
	MAX(CASE WHEN ITEM_VERTICAL = N'Doanh thu' THEN VALUE END) AS [REVENUE],
    MAX(CASE WHEN ITEM_VERTICAL = N'Tăng trưởng doanh thu' THEN VALUE END) AS [REVENUE_GROWTH],
    MAX(CASE WHEN ITEM_VERTICAL = 'Z-score' THEN VALUE END) AS [ZScore],
    MAX(CASE WHEN ITEM_VERTICAL = 'M-score' THEN VALUE END) AS [MScore]
FROM 
    POWER_DATAMART.DBO.FA_FS_TEMP
WHERE 
    GROUP2 IN ('FS_PL_Y', 'FS_GR_Q')
    AND ITEM_VERTICAL IN (
        N'Lợi nhuận sau thuế thu nhập doanh nghiệp', 
        N'Biên lợi nhuận gộp', 
        N'Tăng trưởng LNST', 
		N'Doanh thu', 
        N'Tăng trưởng doanh thu', 
        'Z-score', 
        'M-score'
    )
    AND MAP_PERIOD_DATE = (SELECT MAX(MAP_PERIOD_DATE) FROM POWER_DATAMART.DBO.FA_FS_TEMP WHERE GROUP2 = 'FS_PL_Y' AND ITEM_VERTICAL = N'Lợi nhuận sau thuế thu nhập doanh nghiệp')
	AND LEFT(FIELD_HORIZONTAL, 1) = 'Y' 
	--AND TICKER = 'SSI'
	GROUP BY
    TICKER,
    MAP_PERIOD_DATE,
    CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN CONCAT(YEAR(MAP_PERIOD_DATE), ' Q', (MONTH(MAP_PERIOD_DATE) - 1) / 3 + 1)
        ELSE CAST(YEAR(MAP_PERIOD_DATE) AS VARCHAR(4))
    END,
	CASE 
        WHEN LEFT(FIELD_HORIZONTAL, 1) = 'Q' THEN 'Q'
        ELSE 'Y'
    END 
--ORDER BY PERIOD_TYPE, MAP_PERIOD_STR ASC;
),
BANK_ANNUAL_NII AS
(
SELECT YEAR_PERIOD, STOCK, SUM(NII)/SUM(TRY_CAST(VALUE AS FLOAT)) AS NIM
FROM
(
SELECT CONCAT(LEFT(B.UPDATE_DATE, 4),'12','31') AS YEAR_PERIOD, B.STOCK, B.NIM * A.VALUE NII, A.VALUE
FROM FINANCE_DATA.DBO.FT_BANK B
INNER JOIN (SELECT * 
			FROM POWER_DATAMART.DBO.FA_FS_TEMP 
			WHERE GROUP2 = 'FS_PL_Q' 
			AND ITEM_VERTICAL = N'Thu nhập lãi thuần'
			AND YEAR(MAP_PERIOD_DATE) = (SELECT YEAR(MAX(MAP_PERIOD_DATE)) FROM POWER_DATAMART.DBO.FA_FS_TEMP WHERE GROUP2 = 'FS_PL_Y' AND ITEM_VERTICAL = N'Lợi nhuận sau thuế thu nhập doanh nghiệp')
			) A 
			ON A.TICKER = B.Stock AND CONVERT(VARCHAR(8), A.MAP_PERIOD_DATE, 112) = B.UPDATE_DATE
) A
WHERE STOCK  = 'TCB'
GROUP BY YEAR_PERIOD, STOCK
),
BANK_NII AS
(
SELECT STOCK, NIM
FROM FINANCE_DATA.DBO.FT_BANK B
WHERE STOCK IS NOT NULL
AND B.UPDATE_DATE = (SELECT MAX(UPDATE_DATE) FROM FINANCE_DATA.DBO.FT_BANK WHERE STOCK IS NOT NULL)
),
CYCLE_OrderedData AS (
    SELECT 
        TICKER, 
        TROUGH, 
        DATE_TIME,
        LAG(DATE_TIME) OVER (PARTITION BY TICKER ORDER BY DATE_TIME) AS PreviousDate
    FROM FINPAL_DATA.DBO.CYLE_REDEFINE
		WHERE TICKER = 'AAA'
		AND TROUGH IN ('TROUGH_0', 'TROUGH_AFTER_1') 
),
CYCLE_Days AS (
SELECT 
    TICKER,
    DATEDIFF(DAY, PreviousDate, DATE_TIME) AS DaysofCycle
FROM 
    CYCLE_OrderedData
),
RATING AS (
SELECT A.TICKER
 	, A.CATEGORY
 	, A.SORT
 	, TRY_CAST(A.RATING AS INT) RATING
 	, A.ID
 	, TRY_CAST(PERCENT_RANK() OVER (PARTITION BY CATEGORY ORDER BY RATING ) * 100 AS INT) AS ADD_PERCENTILE
 	, RANK() OVER (PARTITION BY CATEGORY ORDER BY RATING DESC) AS ADD_RANK
 	, COUNT(*) OVER (PARTITION BY CATEGORY ) AS ADD_COUNT
FROM POWER_DATAMART.DBO.TA_STA_RATING A
),
NOTABLE_TRANS AS
(
SELECT DISTINCT 
	[Cổ phiếu] TICKER
	, TRY_CAST((GETDATE() - Ngày) AS INT) [PERIOD]
	, CASE WHEN [Loại giao dịch] = 'Mua' THEN [Số lượng CP] ELSE [Số lượng CP] * -1 END AMT
FROM FINANCE_DATA.DBO.FT_TRANSACTION_DATA
)
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID
, A.PR_Ticker
, A.PR_1W_change AS [Thay đổi giá 1 tuần]
, A.PR_1M_change AS [Thay đổi giá 1 tháng]
, A.PR_3M_change AS [Thay đổi giá 3 tháng]
, A.PR_6M_change AS [Thay đổi giá 6 tháng]
, A.PR_1Y_change AS [Thay đổi giá 1 năm]
, A.PR_pricevsMA20 AS [Giá với TB 20 phiên]
, A.PR_pricevsMA50 AS [Giá với TB 50 phiên]
, A.PR_pricevsMA100 AS [Giá với TB 100 phiên]
, A.PR_Volume AS [Khối lượng phiên gần nhất]
, A.PR_Value AS [Giá trị GD phiên gần nhất]
, A.PR_Vol_MA10 AS [Khối lượng TB 10 phiên]
, A.PR_Vol_MA20 AS [Khối lượng TB 20 phiên]
, A.PR_VolvsMA20 AS [Khối lượng với TB 20 phiên]
, A.PR_VolvsMA50 AS [Khối lượng với TB 50 phiên]
, A.PR_VolvsMA100 AS [Khối lượng với TB 100 phiên]
, A.PR_ActiveBuy AS [Mua chủ động]
, A.PR_Gap_Support AS [Khoảng giá với vùng hỗ trợ]
, A.PR_Gap_Resistance AS [Khoảng giá với vùng kháng cự]
, A.PR_Overall_Support_Resistance AS [Đánh giá Kháng cự - Hỗ trợ]
, A.PR_MACD_conver_diver AS [Hội tụ - Phân kỳ MACD]
, A.PR_MFI_conver_diver AS [Hội tụ - Phân kỳ MFI]
, A.PR_RSI_conver_diver AS [Hội tụ - Phân kỳ RSI]
, A.PR_STO_conver_diver AS [Hội tụ - Phân kỳ Stochastic]
, A.PR_Overall_conver_diver AS [Tổng điểm Hội tụ - Phân kỳ]
, A.PR_RSI AS [RSI]
, A.PR_MFI AS [MFI]
, A.PR_Momentum AS [Momentum]
, A.PR_ADX AS [ADX]
, A.PR_MCDX AS [MCDX]
, A.PR_T_CHANGE_PERC AS [Thay đổi giá phiên gần nhất]
, A.STGY_Point_pricechange_Status AS [Thay đổi giá phiên gần nhất]
, A.STGY_Point_price_Status AS [Mức tăng giá phiên gần nhất]
, A.STGY_Point_candle_Status AS [Nến phiên gần nhất]
, A.STGY_Point_volume_Status AS [Khối lượng phiên gần nhất]
, A.STGY_Point_MFI_Status AS [Dòng tiền phiên gần nhất]
, A.STGY_No_Supply_Status AS [Cạn cung]
, A.STGY_Comulate_MFI_Status AS [Tích lũy dòng tiền]
, A.STGY_Point_overall AS [Phân tích chung phiên gần nhất]
, A.STGY_Comulate_overall AS [Phân tích quá trình tích lũy]

 	, B.TREND [Xu hướng ngắn hạn]
	, D.RENKO_SIGNAL [Tín hiệu đảo chiều]
 	, ROUND(ROUND(B.TP/NULLIF(TRY_CAST(A.PR_CLOSE AS FLOAT), 0)*100 - 100, 0)/ NULLIF(TRY_CAST(A.PR_CLOSE AS FLOAT)/NULLIF(B.CL, 0)*100 - 100, 0),2) [Tỷ lệ cơ hội : rủi ro]
	, CASE
		WHEN (TRY_CAST(E.RM AS FLOAT) > 0 AND TRY_CAST(E.RS AS FLOAT) > 0) THEN N'Dẫn dắt'
		WHEN (TRY_CAST(E.RM AS FLOAT) > 0 AND TRY_CAST(E.RS AS FLOAT) < 0) THEN N'Cải thiện'
		WHEN (TRY_CAST(E.RM AS FLOAT) < 0 AND TRY_CAST(E.RS AS FLOAT) < 0) THEN N'Tụt hậu'
		WHEN (TRY_CAST(E.RM AS FLOAT) < 0 AND TRY_CAST(E.RS AS FLOAT) > 0) THEN N'Suy yếu'
		ELSE NULL END [Giai đoạn]
	, CASE
		WHEN TRY_CAST(E.RM AS FLOAT) > TRY_CAST(F.RM AS FLOAT) THEN N'Động lượng tăng trong 3 phiên gần nhất'
		WHEN TRY_CAST(E.RM AS FLOAT) < TRY_CAST(F.RM AS FLOAT) THEN N'Động lượng giảm trong 3 phiên gần nhất'
		ELSE NULL END [RRG - Động lượng 3 phiên gần nhất]
	, CASE
		WHEN TRY_CAST(E.RM AS FLOAT) > TRY_CAST(F.RM AS FLOAT) THEN N'Tích cực'
		WHEN TRY_CAST(E.RM AS FLOAT) < TRY_CAST(F.RM AS FLOAT) THEN N'Tiêu cực'
		ELSE NULL END [RRG - Động lượng đánh giá]
	, CASE
		WHEN TRY_CAST(E.RS AS FLOAT) > TRY_CAST(F.RS AS FLOAT) THEN N'Sức mạnh tăng trong 3 phiên gần nhất'
		WHEN TRY_CAST(E.RS AS FLOAT) < TRY_CAST(F.RS AS FLOAT) THEN N'Sức mạnh giảm trong 3 phiên gần nhất'
		ELSE NULL END RRG_RS_COMMENT
	, CASE
		WHEN TRY_CAST(E.RS AS FLOAT) > TRY_CAST(F.RS AS FLOAT) THEN N'Tích cực'
		WHEN TRY_CAST(E.RS AS FLOAT) < TRY_CAST(F.RS AS FLOAT) THEN N'Tiêu cực'
		ELSE NULL END [RRG - Sức mạnh đánh giá]
	, TRY_CAST(E.RM AS FLOAT) [RRG - Động lượng]
	, TRY_CAST(E.RS AS FLOAT) [RRG - Sức mạnh]
	, ROUND(TRY_CAST(D.TRANS_NET_BUY_NN AS FLOAT), 3) [Mua ròng NN 1 tuần gần nhất]
	, ROUND(TRY_CAST(D.TRANS_NET_BUY_CTCK AS FLOAT), 3) [Mua ròng TD 1 tuần gần nhất]
	, ROUND(TRY_CAST(C.VAL_PRICE AS FLOAT)/NULLIF(TRY_CAST(C.PR_CLOSE AS FLOAT),0)-1,3)*100 [Định giá Finpal so với Giá thị trường]
	, ROUND(TRY_CAST(H.ZScore AS FLOAT),2) [Z-score]
	, ROUND(TRY_CAST(H.MScore AS FLOAT),2) [M-score]
	, CASE 
		WHEN TRY_CAST(H.ZScore AS FLOAT) = 0 THEN NULL
		WHEN TRY_CAST(H.ZScore AS FLOAT) >= 2.99 THEN N'Tích cực'
		WHEN TRY_CAST(H.ZScore AS FLOAT) >= 1.81 THEN N'Trung lập'
		WHEN TRY_CAST(H.ZScore AS FLOAT) < 1.81 THEN N'Tiêu cực'
		ELSE NULL 
		END [Đánh giá Z-score]
	, CASE 
		WHEN TRY_CAST(H.MScore AS FLOAT) = 0 THEN NULL
		WHEN TRY_CAST(H.MScore AS FLOAT) <= -2.22 THEN N'Tích cực'
		WHEN TRY_CAST(H.MScore AS FLOAT) <= -1.78 THEN N'Trung lập'
		WHEN TRY_CAST(H.MScore AS FLOAT) > -1.78 THEN N'Tiêu cực'
		ELSE NULL 
		END [Đánh giá M-score]
	, ROUND(TRY_CAST(D3.TRANS_NET_BUY_NN AS FLOAT), 3) [Mua ròng NN 1 tháng gần nhất]
	, ROUND(TRY_CAST(D3.TRANS_NET_BUY_CTCK AS FLOAT), 3) [Mua ròng TD 1 tháng gần nhất]
	, ROUND(TRY_CAST(C.TRANS_NET_BUY_NN AS FLOAT),2) [Mua ròng NN Phiên gần nhất]
	, ROUND(TRY_CAST(C.TRANS_NET_BUY_CTCK AS FLOAT),2) [Mua ròng TD Phiên gần nhất]

, J.Loan_to_deposit AS [Cho vay / Tiền gửi (KH)]
, J.Loan_to_asset AS [Cho vay / Tổng tài sản]
, J.NPLsToLoans AS [Nợ xấu / Cho vay]
, J.LoanlossReservesToLoans AS [Trích lập dự phòng / Cho vay]
, J.LoanlossReservesToNPLs AS [Trích lập dự phòng / Nợ xấu]
, J.COF AS [COF]
, J.CostToAssets AS [Chi phí/Tài sản]
, J.CostToIncome AS [Chi phí/Thu nhập]
, J.CostToLoans AS [Chi phí/Cho vay]
, J.ROA AS [ROA]
, J.ROE AS [ROE]
, J.NIM AS [NIM]
, J.PE AS [PE]
, J.PB AS [PB]
, J.Loan_growth AS [Tăng trưởng cho vay]
, J.Deposit_growth AS [Tăng trưởng tiền gửi (KH)]
, J.Net_interest_income_growth AS [Tăng trưởng thu nhập lãi ròng]
, J.Fee_income_growth AS [Tăng trưởng thu nhập phí]
, J.Operating_profit_growth AS [Tăng trưởng thu nhập hoạt động]
, J.Net_profit_growth AS [Tăng trưởng lợi nhuận]
, J.LoansToDeposit_avg_rating AS [Cho vay / Tiền gửi (KH)_ss]
, J.Loan_to_asset_avg_rating AS [Cho vay / Tổng tài sản_ss]
, J.NPLsToLoans_avg_rating AS [Nợ xấu / Cho vay_ss]
, J.LoanlossReservesToLoans_avg_rating AS [Trích lập dự phòng / Cho vay_ss]
, J.LoanlossReservesToNPLs_avg_rating AS [Trích lập dự phòng / Nợ xấu_ss]
, J.COF_avg_rating AS [COF_ss]
, J.CostToAssets_avg_rating AS [Chi phí/Tài sản_ss]
, J.CostToIncome_avg_rating AS [Chi phí/Thu nhập_ss]
, J.CostToLoans_avg_rating AS [Chi phí/Cho vay_ss]
, J.ROA_avg_rating AS [ROA_ss]
, J.ROE_avg_rating AS [ROE_ss]
, J.NIM_avg_rating AS [NIM_ss]
, J.PE_avg_rating AS [PE_ss]
, J.PB_avg_rating AS [PB_ss]
, J.Loan_growth_avg_rating AS [Tăng trưởng cho vay_ss]
, J.Deposit_growth_avg_rating AS [Tăng trưởng tiền gửi (KH)_ss]
, J.Net_interest_income_growth_avg_rating AS [Tăng trưởng thu nhập lãi ròng_ss]
, J.Fee_income_growth_avg_rating AS [Tăng trưởng thu nhập phí_ss]
, J.Operating_profit_growth_avg_rating AS [Tăng trưởng thu nhập hoạt động_ss]
, J.Net_profit_growth_avg_rating AS [Tăng trưởng lợi nhuận_ss]

, K.[Quick_ratio_%] AS [Thanh toán nhanh]
, K.[Current_ratio] AS [Thanh toán ngắn hạn]
, K.[Cash_ratio] AS [Thanh toán bằng tiền]
, K.[Times_Interest_Earned%] AS [Thu nhập / lãi vay]
, K.[Operating_cash_flow_ratio] AS [Thanh toán bằng dòng tiền hoạt động]
, K.[Debt/Equity] AS [Nợ / Vốn chủ sở hữu]
, K.[Debt_ratio] AS [Nợ / Tổng nguồn vốn]
, K.[Equity_ratio] AS [Vốn chủ sở hữu / Tổng nguồn vốn]
, K.[AR_turnover] AS [Vòng quay Phải thu]
, K.[Inventory_turnover] AS [Vòng quay Hàng tồn kho]
, K.[Total_Asset_turnover] AS [Vòng quay Tổng tài sản]
, K.[ROA] AS [ROA]
, K.[ROE] AS [ROE]
, K.[ROI] AS [ROI]
, K.[Gross_profit_margin] AS [Biên lợi nhuận gộp]
, K.[Net_profit_margin] AS [Biên lợi nhuận ròng]
, K.[PE] AS [PE]
, K.[PB] AS [PB]
, K.[Total_asset_growth] AS [Tăng trưởng Tổng tài sản]
, K.[Revenue_growth] AS [Tăng trưởng Doanh thu]
, K.[Net_profit_growth] AS [Tăng trưởng Lợi nhuận]
, K.[Quick_ratio_%_rating] AS [Thanh toán nhanh_ss]
, K.[Current_ratio_rating] AS [Thanh toán ngắn hạn_ss]
, K.[Cash_ratio_rating] AS [Thanh toán bằng tiền_ss]
, K.[Times_Interest_Earned%_rating] AS [Thu nhập / lãi vay_ss]
, K.[Operating_cash_flow_ratio_rating] AS [Thanh toán bằng dòng tiền hoạt động_ss]
, K.[Debt/Equity_rating] AS [Nợ / Vốn chủ sở hữu_ss]
, K.[Debt_ratio_rating] AS [Nợ / Tổng nguồn vốn_ss]
, K.[Equity_ratio_rating] AS [Vốn chủ sở hữu / Tổng nguồn vốn_ss]
, K.[AR_turnover_rating] AS [Vòng quay Phải thu_ss]
, K.[Inventory_turnover_rating] AS [Vòng quay Hàng tồn kho_ss]
, K.[Total_Asset_turnover_rating] AS [Vòng quay Tổng tài sản_ss]
, K.[ROA_rating] AS [ROA_ss]
, K.[ROE_rating] AS [ROE_ss]
, K.[ROI_rating] AS [ROI_ss]
, K.[Gross_profit_margin_rating] AS [Biên lợi nhuận gộp_ss]
, K.[Net_profit_margin_rating] AS [Biên lợi nhuận ròng_ss]
, K.[PE_rating] AS [PE_ss]
, K.[PB_rating] AS [PB_ss]
, K.[Total_asset_growth_rating] AS [Tăng trưởng Tổng tài sản_ss]
, K.[Revenue_growth_rating] AS [Tăng trưởng Doanh thu_ss]
, K.[Net_profit_growth_rating] AS [Tăng trưởng Lợi nhuận_ss]
, L.RATING AS [Tổng điểm]
	, L1.RATING AS [Dòng tiền]
	, L2.RATING AS [Kỹ thuật]
	, L3.RATING AS [Tài chính]
	, CASE WHEN L.ADD_PERCENTILE >= 75 THEN N'Nhóm 25% cao nhất'
 			WHEN L.ADD_PERCENTILE >= 25 THEN N'Nhóm 50% trung bình'
 			WHEN L.ADD_PERCENTILE < 25 THEN N'Nhóm 25% thấp nhất'
 			ELSE NULL
 	END [Tổng điểm_dg]
	, CASE WHEN L1.ADD_PERCENTILE >= 75 THEN N'Nhóm 25% cao nhất'
 			WHEN L1.ADD_PERCENTILE >= 25 THEN N'Nhóm 50% trung bình'
 			WHEN L1.ADD_PERCENTILE < 25 THEN N'Nhóm 25% thấp nhất'
 			ELSE NULL
 	END [Dòng tiền_dg]
	, CASE WHEN L2.ADD_PERCENTILE >= 75 THEN N'Nhóm 25% cao nhất'
 			WHEN L2.ADD_PERCENTILE >= 25 THEN N'Nhóm 50% trung bình'
 			WHEN L2.ADD_PERCENTILE < 25 THEN N'Nhóm 25% thấp nhất'
 			ELSE NULL
 	END [Kỹ thuật_dg]
	, CASE WHEN L3.ADD_PERCENTILE >= 75 THEN N'Nhóm 25% cao nhất'
 			WHEN L3.ADD_PERCENTILE >= 25 THEN N'Nhóm 50% trung bình'
 			WHEN L3.ADD_PERCENTILE < 25 THEN N'Nhóm 25% thấp nhất'
 			ELSE NULL
 	END [Tài chính_dg]
	, ISNULL(N1.VALUE_PRICE/ISNULL(TRY_CAST(A.PR_CLOSE AS FLOAT) * 1000.0, 0), 0) - 1.0 AS [Định giá theo PE so với Giá thị trường]
	, ISNULL(N2.VALUE_PRICE/ISNULL(TRY_CAST(A.PR_CLOSE AS FLOAT) * 1000.0, 0), 0) - 1.0 AS [Định giá theo PB so với Giá thị trường]
	, ISNULL(N3.VALUE_PRICE/ISNULL(TRY_CAST(A.PR_CLOSE AS FLOAT) * 1000.0, 0), 0) - 1.0 AS [Định giá theo Chiết khấu dòng tiền so với Giá thị trường]
	, ISNULL(N4.VALUE_PRICE/ISNULL(TRY_CAST(A.PR_CLOSE AS FLOAT) * 1000.0, 0), 0) - 1.0 AS [Định giá theo Chiết khấu cổ tức so với Giá thị trường]
	, ISNULL(N5.VALUE_PRICE/ISNULL(TRY_CAST(A.PR_CLOSE AS FLOAT) * 1000.0, 0), 0) - 1.0 AS [Định giá theo Lợi nhuận thặng dư so với Giá thị trường]
	, (TRY_CAST(PR_BuyTAScore AS FLOAT) + TRY_CAST(PR_NeutralTAScore AS FLOAT)- TRY_CAST(PR_SellTAScore AS FLOAT) +16)/32*100 AS [Tổng điểm kỹ thuật]
	, O.AMT [Mua ròng GDCY 1 Quý gần nhất]
	, O1.AMT [Mua ròng GDCY 6 Tháng gần nhất]
	, O2.AMT [Mua ròng GDCY 1 Năm gần nhất]
FROM POWER_DATAMART.DBO.TA_STA A
LEFT JOIN POWER_DATAMART.DBO.TA_STA_STRATEGY B ON A.PR_Ticker = B.PR_Ticker
LEFT JOIN TA_FT C ON A.PR_Ticker = C.PR_TICKER
LEFT JOIN TA_FT_2 D ON A.PR_Ticker = D.PR_TICKER
LEFT JOIN TA_FT_3 D3 ON A.PR_Ticker = D3.PR_TICKER
LEFT JOIN (SELECT * FROM POWER_DATAMART.DBO.TA_RRG WHERE PERIOD = 'T')  E ON A.PR_Ticker = E.TICKER 
LEFT JOIN (SELECT * FROM POWER_DATAMART.DBO.TA_RRG WHERE PERIOD = 'T-3') F ON A.PR_Ticker = F.TICKER 
LEFT JOIN FA_FS G ON A.PR_TICKER = G.TICKER
LEFT JOIN FA_FS_Y H ON A.PR_TICKER = H.TICKER
LEFT JOIN BANK_NII QB ON QB.Stock = A.PR_TICKER
LEFT JOIN BANK_ANNUAL_NII YB ON A.PR_TICKER = YB.Stock
LEFT JOIN (SELECT * FROM CYCLE_Days WHERE DaysofCycle IS NOT NULL) I ON A.PR_Ticker = I.TICKER
LEFT JOIN (SELECT * FROM FINANCE_DATA.DBO.Bank WHERE STOCK IS NOT NULL) J ON A.PR_TICKER = J.Stock
LEFT JOIN (SELECT * FROM FINANCE_DATA.DBO.NON_Bank WHERE INDUSTRY NOT IN ('8355', (SELECT DISTINCT INDUSTRY FROM FINANCE_DATA.DBO.Non_Bank WHERE Stock = 'TCB'))) K ON A.PR_TICKER = K.STOCK
LEFT JOIN (SELECT * FROM RATING WHERE CATEGORY = N'Tổng điểm') L ON A.PR_Ticker = L.TICKER
LEFT JOIN (SELECT * FROM RATING WHERE CATEGORY = N'Dòng tiền') L1 ON A.PR_Ticker = L1.TICKER
LEFT JOIN (SELECT * FROM RATING WHERE CATEGORY = N'Kỹ thuật') L2 ON A.PR_Ticker = L2.TICKER
LEFT JOIN (SELECT * FROM RATING WHERE CATEGORY = N'Tài chính') L3 ON A.PR_Ticker = L3.TICKER
LEFT JOIN (SELECT * FROM VALUATION_DETAIL WHERE METHOD = 'PE') N1 ON A.PR_Ticker = N1.Ticker
LEFT JOIN (SELECT * FROM VALUATION_DETAIL WHERE METHOD = 'PB') N2 ON A.PR_Ticker = N2.Ticker
LEFT JOIN (SELECT * FROM VALUATION_DETAIL WHERE METHOD = N'Chiết khấu dòng tiền') N3 ON A.PR_Ticker = N3.Ticker
LEFT JOIN (SELECT * FROM VALUATION_DETAIL WHERE METHOD = N'Chiết khấu cổ tức') N4 ON A.PR_Ticker = N4.Ticker
LEFT JOIN (SELECT * FROM VALUATION_DETAIL WHERE METHOD = N'Lợi nhuận thặng dự') N5 ON A.PR_Ticker = N5.Ticker
LEFT JOIN (SELECT TICKER, SUM(AMT) AMT FROM NOTABLE_TRANS WHERE PERIOD < 90 GROUP BY TICKER) O ON A.PR_Ticker = O.TICKER
LEFT JOIN (SELECT TICKER, SUM(AMT) AMT  FROM NOTABLE_TRANS WHERE PERIOD < 180 GROUP BY TICKER) O1 ON A.PR_Ticker = O1.TICKER
LEFT JOIN (SELECT TICKER, SUM(AMT) AMT  FROM NOTABLE_TRANS WHERE PERIOD < 360 GROUP BY TICKER) O2 ON A.PR_Ticker = O2.TICKER
    WHERE {where_clause_common}
    """
    result_ta_sta = execute_sql_query(query_ta_sta, parameters_common)

    # Combine the result into a dictionary
    combined_result = {
        'TA_STA': result_ta_sta
    }

    return jsonify({'data': combined_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
