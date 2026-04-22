
import io
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd

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

def parse_user_date(text: str) -> date:
    text = str(text).strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return pd.to_datetime(text).date()

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

def build_date_column_map(df: pd.DataFrame):
    mapping = {}
    for c in df.columns:
        d = parse_header_to_date(c)
        if d is not None:
            mapping[d] = c
    return mapping

def date_range_saturdays(start_date: date, end_date: date):
    out = []
    cur = start_date
    while cur <= end_date:
        out.append(cur)
        cur += timedelta(days=7)
    return out

def get_numeric(df: pd.DataFrame, col):
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)

def group_value(df: pd.DataFrame, key_cols, value_col, output_name):
    if df.empty:
        return pd.DataFrame(columns=key_cols + [output_name])
    out = df.groupby(key_cols, dropna=False, as_index=False)[value_col].sum()
    return out.rename(columns={value_col: output_name})

def transform_df(raw: pd.DataFrame, target_week: date, current_week: date):
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
    target_col = date_col_map.get(target_week)
    if target_col is None:
        raise ValueError(f"Không tìm thấy cột Target Week trong Sheet1: {fmt_header_date(target_week)}")

    all_week_dates = sorted(date_col_map.keys())
    if not all_week_dates:
        raise ValueError("Không tìm thấy các cột tuần trong Sheet1.")

    first_week_date = min(all_week_dates)
    planned_weeks = date_range_saturdays(first_week_date, target_week)
    planned_missing = [fmt_header_date(d) for d in planned_weeks if d not in date_col_map]
    if planned_missing:
        raise ValueError("Thiếu cột tuần cho Planned POS: " + ", ".join(planned_missing))

    if current_week > target_week:
        raise ValueError("Target Week phải lớn hơn hoặc bằng Current Week.")

    net_weeks = date_range_saturdays(current_week, target_week)
    net_missing = [fmt_header_date(d) for d in net_weeks if d not in date_col_map]
    if net_missing:
        raise ValueError("Thiếu cột tuần cho NET FCST: " + ", ".join(net_missing))

    planned_cols = [date_col_map[d] for d in planned_weeks]
    net_cols = [date_col_map[d] for d in net_weeks]
    key_cols = ["Item #", "Whse", "Coll. Class"]

    si = raw[raw["Data Type"] == "SHIPPABLE INV"].copy()
    si["Base_SI"] = get_numeric(si, target_col)
    si_g = group_value(si, key_cols, "Base_SI", "Base_SI")

    planned = raw[raw["Data Type"] == "PLANNED POS"].copy()
    planned["PlannedPO_Sum"] = sum((get_numeric(planned, c) for c in planned_cols), start=pd.Series(0.0, index=planned.index))
    planned_g = group_value(planned, key_cols, "PlannedPO_Sum", "PlannedPO_Sum")

    firm = raw[raw["Data Type"] == "FIRM POS"].copy()
    firm["FirmPO_Target"] = get_numeric(firm, target_col)
    firm_g = group_value(firm, key_cols, "FirmPO_Target", "FirmPO_Target")

    net_fcst = raw[raw["Data Type"] == "NET FCST"].copy()
    net_fcst["NetFcst_Sum"] = sum((get_numeric(net_fcst, c) for c in net_cols), start=pd.Series(0.0, index=net_fcst.index))
    net_fcst_g = group_value(net_fcst, key_cols, "NetFcst_Sum", "NetFcst_Sum")

    ss = raw[raw["Data Type"] == "SAFETY STK"].copy()
    ss["SS_Wk3"] = get_numeric(ss, target_col)
    ss_g = group_value(ss, key_cols, "SS_Wk3", "SS_Wk3")

    base = raw[raw["Data Type"].isin(["SHIPPABLE INV", "PLANNED POS", "FIRM POS", "NET FCST", "SAFETY STK"])][key_cols].drop_duplicates()

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
        ["First week in file", fmt_header_date(first_week_date)],
        ["TargetWeek", fmt_header_date(target_week)],
        ["CurrentWeek", fmt_header_date(current_week)],
        ["Planned POS range", ", ".join(fmt_header_date(d) for d in planned_weeks)],
        ["NET FCST range", ", ".join(fmt_header_date(d) for d in net_weeks)],
        ["TargetWeek column found", str(target_col)],
        ["Whse 335 logic", "SI(Target Week) - Planned POS(First week in file -> Target Week) - Firm POS(Target Week) + Net Fcst(Current Week -> Target Week)"],
        ["Other Whse logic", "SI(Target Week) - Planned POS(First week in file -> Target Week) - Firm POS(Target Week)"],
        ["Rows output", str(len(out))],
    ]
    debug_df = pd.DataFrame(debug_rows, columns=["Field", "Value"])
    return out, debug_df

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

def load_raw_sheet(path: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name="Sheet1")
    except Exception:
        return pd.read_excel(path, sheet_name=0)

def transform_file(input_path: str, output_path: str, target_week: date, current_week: date):
    raw = load_raw_sheet(input_path)
    out, debug_df = transform_df(raw, target_week, current_week)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Output", index=False)
        debug_df.to_excel(writer, sheet_name="Debug", index=False)

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Destination Change Builder - Tkinter")
        self.root.geometry("940x680")
        self.root.minsize(900, 620)
        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        default_current = saturday_of_current_week()
        default_target = default_current + timedelta(days=14)
        self.target_var = tk.StringVar(value=fmt_header_date(default_target))
        self.current_var = tk.StringVar(value=fmt_header_date(default_current))
        self.status_var = tk.StringVar(value="San sang")
        self.build_ui()

    def build_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 15, "bold"))
        style.configure("Big.TButton", font=("Segoe UI", 12, "bold"), padding=(16, 12))
        style.configure("Small.TButton", font=("Segoe UI", 10), padding=(10, 8))
        style.configure("Status.TLabel", font=("Segoe UI", 10, "italic"))

        outer = ttk.Frame(self.root, padding=16)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text="Destination Change Builder", style="Title.TLabel").pack(anchor="w", pady=(0, 12))

        file_frame = ttk.LabelFrame(outer, text="File")
        file_frame.pack(fill="x", pady=(0, 12))
        ttk.Label(file_frame, text="Input file (Raw data - Sheet1):").grid(row=0, column=0, sticky="w", padx=10, pady=(10, 4))
        ttk.Entry(file_frame, textvariable=self.input_var, width=85).grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        ttk.Button(file_frame, text="Browse", command=self.browse_input, style="Small.TButton").grid(row=1, column=1, sticky="e", padx=10, pady=(0, 10))
        ttk.Label(file_frame, text="Output file:").grid(row=2, column=0, sticky="w", padx=10, pady=(2, 4))
        ttk.Entry(file_frame, textvariable=self.output_var, width=85).grid(row=3, column=0, sticky="ew", padx=10, pady=(0, 10))
        ttk.Button(file_frame, text="Save As", command=self.browse_output, style="Small.TButton").grid(row=3, column=1, sticky="e", padx=10, pady=(0, 10))
        file_frame.columnconfigure(0, weight=1)

        date_frame = ttk.LabelFrame(outer, text="Date setup")
        date_frame.pack(fill="x", pady=(0, 12))
        ttk.Label(date_frame, text="Target Week:").grid(row=0, column=0, sticky="w", padx=10, pady=(12, 6))
        ttk.Entry(date_frame, textvariable=self.target_var, width=24).grid(row=0, column=1, sticky="w", padx=10, pady=(12, 6))
        ttk.Label(date_frame, text="Current Week (Saturday of this week):").grid(row=1, column=0, sticky="w", padx=10, pady=6)
        ttk.Entry(date_frame, textvariable=self.current_var, width=24).grid(row=1, column=1, sticky="w", padx=10, pady=6)
        ttk.Button(date_frame, text="Reset Current Week = This Week Saturday", command=self.reset_current_week, style="Small.TButton").grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=(10, 12))

        logic_frame = ttk.LabelFrame(outer, text="Logic")
        logic_frame.pack(fill="x", pady=(0, 12))
        logic_text = (
            "Whse = 335:\n"
            "Sum of SI Wk3 = SHIPPABLE INV tai Target Week - Total PLANNED POS tu tuan dau tien trong file den Target Week "
            "- FIRM POS tai Target Week + Total NET FCST tu Current Week den Target Week\n\n"
            "Whse <> 335:\n"
            "Sum of SI Wk3 = SHIPPABLE INV tai Target Week - Total PLANNED POS tu tuan dau tien trong file den Target Week "
            "- FIRM POS tai Target Week\n\n"
            "Sum of SI-SS Wk3 = Sum of SI Wk3 - SAFETY STK tai Target Week"
        )
        ttk.Label(logic_frame, text=logic_text, justify="left", wraplength=860).pack(anchor="w", padx=10, pady=10)

        action_frame = ttk.Frame(outer)
        action_frame.pack(fill="x", pady=(6, 0))
        ttk.Label(action_frame, textvariable=self.status_var, style="Status.TLabel").pack(side="left")
        ttk.Button(action_frame, text="RUN", command=self.run, style="Big.TButton").pack(side="right")

    def browse_input(self):
        path = filedialog.askopenfilename(title="Select input Excel file", filetypes=[("Excel files", "*.xlsx *.xlsm *.xls"), ("All files", "*.*")])
        if path:
            self.input_var.set(path)
            if not self.output_var.get().strip():
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.output_var.set(str(Path(path).with_name(f"destination_change_output_{stamp}.xlsx")))
            self.status_var.set("Da chon input file")

    def browse_output(self):
        path = filedialog.asksaveasfilename(title="Save output Excel file", defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if path:
            self.output_var.set(path)
            self.status_var.set("Da chon noi luu output")

    def reset_current_week(self):
        self.current_var.set(fmt_header_date(saturday_of_current_week()))
        self.status_var.set("Da reset Current Week")

    def run(self):
        try:
            input_path = self.input_var.get().strip()
            output_path = self.output_var.get().strip()
            if not input_path:
                raise ValueError("Ban chua chon file input.")
            if not output_path:
                raise ValueError("Ban chua chon noi luu output.")
            target_week = parse_user_date(self.target_var.get())
            current_week = parse_user_date(self.current_var.get())
            self.status_var.set("Dang xu ly...")
            self.root.update_idletasks()
            transform_file(input_path, output_path, target_week, current_week)
            self.status_var.set("Hoan tat")
            messagebox.showinfo("Done", f"Da tao file output:\n{output_path}")
        except Exception as e:
            self.status_var.set("Co loi")
            messagebox.showerror("Error", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
