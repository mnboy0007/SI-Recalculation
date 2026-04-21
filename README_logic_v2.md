# Destination Change Builder

## Run locally
```bash
pip install -r requirements.txt
streamlit run app_logic_v2.py
```

## Deploy to Streamlit Cloud
- Upload `app_logic_v2.py` and `requirements.txt` to GitHub
- In Streamlit Cloud, create a new app from that repo
- Main file path: `app_logic_v2.py`

## Logic
- Whse = 335  
  `Sum of SI Wk3 = SHIPPABLE INV tại Target Week - Total PLANNED POS từ Current Week đến Target Week - FIRM POS tại Target Week + Total NET FCST từ Current Week đến Target Week`

- Whse <> 335  
  `Sum of SI Wk3 = SHIPPABLE INV tại Target Week - Total PLANNED POS từ Current Week đến Target Week - FIRM POS tại Target Week`

- `Sum of SI-SS Wk3 = Sum of SI Wk3 - SAFETY STK tại Target Week`
