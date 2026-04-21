import io
from datetime import datetime, timedelta, date

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Destination Change Builder",
    page_icon="📦",
    layout="wide",
)

OUTPUT_COLUMNS = [
    "Item",
    "ProdResourceID",
    "Whse",
    "F Wk3",
    "Sum of SI Wk3",
    "Sum of SI-SS Wk3",
    "Average of SS Wk3",
    "Vendor",
]

DTYPE_MAP = {
    "FIRM DEMAND": "FIRM DEMANDS",
    "FIRM DEMANDS": "FIRM DEMANDS",
    "FIRM POS": "FIRM POS",
    "FIRM PO": "FIRM POS",
    "PLANNED POS": "PLANNED POS",
    "PLANNED PO": "PLANNED POS",
    "SHIPPABLE INV": "SHIPPABLE INV",
    "SHIPPABLE INVENTORY": "SHIPPABLE INV",
    "SAFETY STK": "SAFETY STK",
    "SAFETY STOCK": "SAFETY STK",
    "NET FCST": "NET FCST",
    "NET FORECAST": "NET FCST",
}


def saturday_of_current_week(today=None):
    if today is None:
        today = date.today()
    days_until_saturday = (5 - today.weekday()) % 7
    return today + timedelta(days=days_until_saturday)


def fmt_header_date(d: date) -> str:
    return f"{d.month}/{d.day}/{d.year}"


def parse_header_to_date(col_name):
    if isinstance(col_name, (datetime, pd.Timestamp)):
        return pd.to_datetime(col_name).date()
    text = str(col_name).strip()
    try:
        return pd.to_datetime(text).date()
    except Exception:
        return None


def clean_dtype(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip().str.upper()
    return s.map(lambda x: DTYPE_MAP.get(x, x))


def load_raw_sheet(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    try:
        return pd.read_excel(uploaded_file, sheet_name="Sheet1")
    except Exception:
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file, sheet_name=0)


def build_date_column_map(df: pd.DataFrame):
    mapping = {}
    for col in df.columns:
        d = parse_header_to_date(col)
        if d is not None:
            mapping[d] = col
    return mapping


def date_range_saturdays(start_date: date, end_date: date):
    result = []
    current = start_date
    while current <= end_date:
        result.append(current)
        current += timedelta(days=7)
    return result


def get_numeric(df: pd.DataFrame, col):
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def group_value(df: pd.DataFrame, key_cols, value_col, output_name):
    if df.empty:
        return pd.DataFrame(columns=key_cols + [output_name])
    out = df.groupby(key_cols, dropna=False, as_index=False)[value_col].sum()
    return out.rename(columns={value_col: output_name})


def transform_file(uploaded_file, target_week: date, current_week: date):
    raw = load_raw_sheet(uploaded_file)
    raw.columns = [str(c).strip() for c in raw.columns]

    required_base = ["Item #", "Whse", "Data Type", "Coll. Class", "MakeBuy Code"]
    missing_base = [c for c in required_base if c not in raw.columns]
    if missing_base:
        raise ValueError(f"Thiếu cột bắt buộc trong Sheet1: {', '.join(missing_base)}")

    raw["Data Type"] = clean_dtype(raw["Data Type"])
    raw["MakeBuy Code"] = raw["MakeBuy Code"].fillna("").astype(str).str.strip().str.upper()
    raw["Item #"] = raw["Item #"].fillna("").astype(str).str.strip()
    raw["Whse"] = raw["Whse"].fillna("").astype(str).str.strip()
    raw["Coll. Class"] = raw["Coll. Class"].fillna("").astype(str).str.strip()

    raw = raw[raw["MakeBuy Code"] == "B"].copy()
    if raw.empty:
        raise ValueError("Không có dòng dữ liệu nào sau khi lọc MakeBuy Code = B.")

    date_col_map = build_date_column_map(raw)
    current_col = date_col_map.get(current_week)
    target_col = date_col_map.get(target_week)

    if current_col is None:
        raise ValueError(f"Không tìm thấy cột Current Week trong Sheet1: {fmt_header_date(current_week)}")
    if target_col is None:
        raise ValueError(f"Không tìm thấy cột Target Week trong Sheet1: {fmt_header_date(target_week)}")
    if current_week > target_week:
        raise ValueError("Target Week phải lớn hơn hoặc bằng Current Week.")

    calc_weeks = date_range_saturdays(current_week, target_week)
    calc_missing = [fmt_header_date(d) for d in calc_weeks if d not in date_col_map]
    if calc_missing:
        raise ValueError("Thiếu cột tuần trong Sheet1: " + ", ".join(calc_missing))

    calc_cols = [date_col_map[d] for d in calc_weeks]
    key_cols = ["Item #", "Whse", "Coll. Class"]

    si = raw[raw["Data Type"] == "SHIPPABLE INV"].copy()
    si["Base_SI"] = get_numeric(si, target_col)
    si_g = group_value(si, key_cols, "Base_SI", "Base_SI")

    planned = raw[raw["Data Type"] == "PLANNED POS"].copy()
    planned["PlannedPO_Sum"] = sum((get_numeric(planned, c) for c in calc_cols), start=pd.Series(0.0, index=planned.index))
    planned_g = group_value(planned, key_cols, "PlannedPO_Sum", "PlannedPO_Sum")

    firm = raw[raw["Data Type"] == "FIRM POS"].copy()
    firm["FirmPO_Target"] = get_numeric(firm, target_col)
    firm_g = group_value(firm, key_cols, "FirmPO_Target", "FirmPO_Target")

    net_fcst = raw[raw["Data Type"] == "NET FCST"].copy()
    net_fcst["NetFcst_Sum"] = sum((get_numeric(net_fcst, c) for c in calc_cols), start=pd.Series(0.0, index=net_fcst.index))
    net_fcst_g = group_value(net_fcst, key_cols, "NetFcst_Sum", "NetFcst_Sum")

    ss = raw[raw["Data Type"] == "SAFETY STK"].copy()
    ss["SS_Wk3"] = get_numeric(ss, target_col)
    ss_g = group_value(ss, key_cols, "SS_Wk3", "SS_Wk3")

    base = raw[
        raw["Data Type"].isin(["SHIPPABLE INV", "PLANNED POS", "FIRM POS", "NET FCST", "SAFETY STK"])
    ][key_cols].drop_duplicates()

    out = (
        base.merge(si_g, on=key_cols, how="left")
        .merge(planned_g, on=key_cols, how="left")
        .merge(firm_g, on=key_cols, how="left")
        .merge(net_fcst_g, on=key_cols, how="left")
        .merge(ss_g, on=key_cols, how="left")
    )

    for col in ["Base_SI", "PlannedPO_Sum", "FirmPO_Target", "NetFcst_Sum", "SS_Wk3"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    out["Whse_text"] = out["Whse"].fillna("").astype(str).str.strip()

    out["F Wk3"] = 0.0
    out["Sum of SI Wk3"] = out["Base_SI"] - out["PlannedPO_Sum"] - out["FirmPO_Target"]

    mask_335 = out["Whse_text"] == "335"
    out.loc[mask_335, "Sum of SI Wk3"] = (
        out.loc[mask_335, "Base_SI"]
        - out.loc[mask_335, "PlannedPO_Sum"]
        - out.loc[mask_335, "FirmPO_Target"]
        + out.loc[mask_335, "NetFcst_Sum"]
    )

    out["Sum of SI-SS Wk3"] = out["Sum of SI Wk3"] - out["SS_Wk3"]
    out["Average of SS Wk3"] = out["SS_Wk3"]
    out["Vendor"] = None

    out = out.rename(columns={"Item #": "Item", "Coll. Class": "ProdResourceID"})
    out = out[OUTPUT_COLUMNS].drop_duplicates()

    debug_rows = [
        ["TargetWeek", fmt_header_date(target_week)],
        ["CurrentWeek", fmt_header_date(current_week)],
        ["Calculation range", ", ".join(fmt_header_date(d) for d in calc_weeks)],
        ["CurrentWeek column found", str(current_col)],
        ["TargetWeek column found", str(target_col)],
        ["Whse 335 logic", "SI(Target Week) - Planned POS(Current Week -> Target Week) - Firm POS(Target Week) + Net Fcst(Current Week -> Target Week)"],
        ["Other Whse logic", "SI(Target Week) - Planned POS(Current Week -> Target Week) - Firm POS(Target Week)"],
        ["Rows output", str(len(out))],
    ]
    debug_df = pd.DataFrame(debug_rows, columns=["Field", "Value"])
    return out, debug_df


def build_excel_bytes(output_df: pd.DataFrame, debug_df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        output_df.to_excel(writer, sheet_name="Output", index=False)
        debug_df.to_excel(writer, sheet_name="Debug", index=False)
    buffer.seek(0)
    return buffer.read()


st.title("Destination Change Builder")

with st.expander("Logic đang áp dụng", expanded=True):
    st.markdown(
        '''
- **Whse = 335**  
  `Sum of SI Wk3 = SHIPPABLE INV tại Target Week - Total PLANNED POS từ Current Week đến Target Week - FIRM POS tại Target Week + Total NET FCST từ Current Week đến Target Week`

- **Whse <> 335**  
  `Sum of SI Wk3 = SHIPPABLE INV tại Target Week - Total PLANNED POS từ Current Week đến Target Week - FIRM POS tại Target Week`

- `Sum of SI-SS Wk3 = Sum of SI Wk3 - SAFETY STK tại Target Week`
- `Average of SS Wk3 = SAFETY STK tại Target Week`
- `F Wk3 = 0`
- `Vendor` để trống
- Lọc `MakeBuy Code = B`
'''
    )

default_current = saturday_of_current_week()
default_target = default_current + timedelta(days=14)

left, right = st.columns(2)
with left:
    uploaded_file = st.file_uploader("Upload file Excel raw", type=["xlsx", "xlsm", "xls"])
    current_week = st.date_input("Current Week", value=default_current, format="MM/DD/YYYY")
with right:
    target_week = st.date_input("Target Week", value=default_target, format="MM/DD/YYYY")
    st.text_input("Sheet source", value="Sheet1", disabled=True)

run = st.button("Run", type="primary", use_container_width=True)

if run:
    if uploaded_file is None:
        st.error("Bạn chưa upload file input.")
    else:
        try:
            with st.spinner("Đang xử lý dữ liệu..."):
                output_df, debug_df = transform_file(uploaded_file, target_week, current_week)
                excel_bytes = build_excel_bytes(output_df, debug_df)

            st.success("Đã xử lý xong.")
            preview_col, debug_col = st.columns([3, 2])

            with preview_col:
                st.subheader("Output preview")
                st.dataframe(output_df, use_container_width=True, height=450)

            with debug_col:
                st.subheader("Debug")
                st.dataframe(debug_df, use_container_width=True, height=450)

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="Download output Excel",
                data=excel_bytes,
                file_name=f"destination_change_output_{stamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.error(str(e))
