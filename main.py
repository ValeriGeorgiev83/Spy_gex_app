import os
import math
import json
import requests
import pandas as pd
import numpy as np
import flet as ft
import threading
import time
import yfinance as yf
from datetime import datetime, timezone, timedelta 

# Initialize Upstash Redis with your EXACT verified connection parameters
from upstash_redis import Redis
redis = Redis(
    url="https://large-ghost-131173.upstash.io",
    token="gQAAAAAAAgBlAAIgcDE2NmI0NGZkNDFiYTk0NzlhOWJmZGM1MTg5OWViZDIxMw"
)
REDIS_FLOW_KEY = "spy_flow_24h_history"
REDIS_OI_MIGRATION_KEY = "spy_oi_hourly_history"

last_known_atm_iv = [20.0] 

def native_norm_pdf(x):
    return (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * (x ** 2)) 

def native_norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0))) 

def calculate_speed_for_option(spot, strike, iv, t_days, oi, option_type):
    if t_days <= 0 or iv <= 0 or oi <= 0:
        return 0.0
    try:
        t = t_days / 365.0
        d1 = (math.log(spot / strike) + (0.5 * (iv ** 2)) * t) / (iv * math.sqrt(t))
        pdf = native_norm_pdf(d1) 
        gamma = pdf / (spot * iv * math.sqrt(t))
        speed_per_contract = (-gamma / spot) * (1.0 + (d1 / (iv * math.sqrt(t)))) 
        footprint = oi * speed_per_contract * 0.01 * 100.0
        return -footprint if option_type == 'P' else footprint
    except Exception:
        return 0.0 

def calculate_realized_vol_10d(ticker_obj):
    try:
        hist = ticker_obj.history(period="15d")
        if len(hist) < 10:
            return 15.0
        closes = hist['Close'].tail(10).values
        log_returns = np.diff(np.log(closes))
        daily_std = np.std(log_returns, ddof=1)
        return float(daily_std * math.sqrt(252) * 100.0)
    except Exception:
        return 15.0 

def background_data_worker(symbol="SPY"):
    print("Background Upstash Processing Worker Loop Engaged.")
    ticker = yf.Ticker(symbol)
    
    while True:
        try:
            spot_price = float(ticker.fast_info['lastPrice'])
            expirations = ticker.options
            if not expirations:
                time.sleep(10)
                continue
                
            now = datetime.now(timezone.utc)
            target_expiries = expirations[:5]
            
            current_call_volume_premium = 0.0
            current_put_volume_premium = 0.0
            net_delta_premium_drift = 0.0
            parsed_options = []
            
            for expiry_str in target_expiries:
                try:
                    expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).replace(hour=16, minute=0)
                    days_to_expiry = (expiry_dt - now).total_seconds() / 86400.0
                    if days_to_expiry < 0: continue
                except Exception: continue
                
                try:
                    chain = ticker.option_chain(expiry_str)
                    calls = chain.calls
                    puts = chain.puts
                except Exception: continue
                
                for _, row in calls.iterrows():
                    strike = float(row['strike'])
                    oi = float(row.get('openInterest', 0))
                    vol = float(row.get('volume', 0)) if not pd.isna(row.get('volume', 0)) else 0.0
                    iv = float(row.get('impliedVolatility', 0.15))
                    last_price = float(row.get('lastPrice', 0.0))
                    
                    if oi > 0:
                        parsed_options.append({'strike': strike, 'oi': oi, 'days_to_expiry': days_to_expiry, 'type': 'C'})
                    
                    notional_value = vol * last_price * 100.0
                    current_call_volume_premium += notional_value
                    
                    try:
                        t_trade = max(days_to_expiry, 0.01) / 365.0
                        d1_trade = (math.log(spot_price / strike) + (0.5 * (iv ** 2)) * t_trade) / (iv * math.sqrt(t_trade))
                        delta = native_norm_cdf(d1_trade)
                    except Exception: delta = 0.5
                    net_delta_premium_drift += (delta * notional_value)

                for _, row in puts.iterrows():
                    strike = float(row['strike'])
                    oi = float(row.get('openInterest', 0))
                    vol = float(row.get('volume', 0)) if not pd.isna(row.get('volume', 0)) else 0.0
                    iv = float(row.get('impliedVolatility', 0.15))
                    last_price = float(row.get('lastPrice', 0.0))
                    
                    if oi > 0:
                        parsed_options.append({'strike': strike, 'oi': oi, 'days_to_expiry': days_to_expiry, 'type': 'P'})
                        
                    notional_value = vol * last_price * 100.0
                    current_put_volume_premium += notional_value
                    
                    try:
                        t_trade = max(days_to_expiry, 0.01) / 365.0
                        d1_trade = (math.log(spot_price / strike) + (0.5 * (iv ** 2)) * t_trade) / (iv * math.sqrt(t_trade))
                        delta = native_norm_cdf(d1_trade) - 1.0
                    except Exception: delta = -0.5
                    net_delta_premium_drift += (delta * notional_value)

            current_ts = now.strftime("%m-%d %H:%M")
            last_logged_element = redis.lindex(REDIS_FLOW_KEY, -1)
            
            inc_call_flow, inc_put_flow, inc_drift = 0.0, 0.0, 0.0
            
            if last_logged_element:
                prev_data = json.loads(last_logged_element)
                prev_raw_call = prev_data.get("raw_call_accumulator", current_call_volume_premium)
                prev_raw_put = prev_data.get("raw_put_accumulator", current_put_volume_premium)
                prev_raw_drift = prev_data.get("raw_drift_accumulator", net_delta_premium_drift)
                
                if current_call_volume_premium >= prev_raw_call:
                    inc_call_flow = current_call_volume_premium - prev_raw_call
                else: inc_call_flow = current_call_volume_premium
                
                if current_put_volume_premium >= prev_raw_put:
                    inc_put_flow = current_put_volume_premium - prev_raw_put
                else: inc_put_flow = current_put_volume_premium
                
                inc_drift = net_delta_premium_drift - prev_raw_drift

            flow_snapshot = {
                "timestamp": current_ts,
                "call_flow": round(inc_call_flow, 2),
                "put_flow": round(inc_put_flow, 2),
                "ndf_drift": round(inc_drift, 2),
                "c_ask": round(inc_call_flow * 0.52, 2),
                "c_bid": round(inc_call_flow * 0.48, 2),
                "p_ask": round(inc_put_flow * 0.52, 2),
                "p_bid": round(inc_put_flow * 0.48, 2),
                "raw_call_accumulator": current_call_volume_premium,
                "raw_put_accumulator": current_put_volume_premium,
                "raw_drift_accumulator": net_delta_premium_drift
            }
            redis.rpush(REDIS_FLOW_KEY, json.dumps(flow_snapshot))
            redis.ltrim(REDIS_FLOW_KEY, -288, -1)

            if now.minute <= 4:
                hourly_time_tag = now.strftime("%m-%d %H:%M")
                base_df = pd.DataFrame(parsed_options)
                oi_snapshot_map = {}
                if not base_df.empty:
                    base_df['strike_bucket'] = base_df['strike'].apply(lambda x: round(x))
                    oi_snapshot_map = base_df.groupby('strike_bucket')['oi'].sum().to_dict()
                    oi_snapshot_map = {str(k): float(v) for k, v in oi_snapshot_map.items()}

                oi_history_snapshot = {"timestamp": hourly_time_tag, "oi_distribution": oi_snapshot_map}
                redis.rpush(REDIS_OI_MIGRATION_KEY, json.dumps(oi_history_snapshot))
                redis.ltrim(REDIS_OI_MIGRATION_KEY, -168, -1)

            print("Background state sync complete.")
        except Exception as loop_ex:
            print(f"Background Loop Error: {loop_ex}")
            
        time.sleep(300)

def fetch_deribit_gex(symbol="SPY"):
    try:
        ticker = yf.Ticker(symbol)
        spot_price = float(ticker.fast_info['lastPrice'])
        expirations = ticker.options
    except Exception: return None 

    now = datetime.now(timezone.utc)
    parsed_options = []
    atm_iv = 15.0
    min_strike_dist = float('inf')
    net_charm_accumulator = 0.0 

    net_speed_current, net_speed_down_10, net_speed_up_10 = 0.0, 0.0, 0.0

    for expiry_str in expirations[:3]:
        try:
            chain = ticker.option_chain(expiry_str)
            expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).replace(hour=16, minute=0)
            days_to_expiry = (expiry_dt - now).total_seconds() / 86400.0
            if days_to_expiry < 0: continue
        except Exception: continue

        calls = chain.calls.copy()
        calls['type'] = 'C'
        puts = chain.puts.copy()
        puts['type'] = 'P'
        combined = pd.concat([calls, puts])

        for _, row in combined.iterrows():
            strike = float(row['strike'])
            option_type = row['type']
            oi = float(row.get('openInterest', 0))
            volume = float(row.get('volume', 0)) if not pd.isna(row.get('volume', 0)) else 0.0
            iv = float(row.get('impliedVolatility', 15.0))
            if iv <= 0: iv = 0.15

            if days_to_expiry <= 5.0:
                dist = abs(spot_price - strike)
                if dist < min_strike_dist:
                    min_strike_dist = dist
                    atm_iv = iv * 100.0 

            try:
                t_days = max(days_to_expiry, 0.01) / 365.0
                distance = abs(math.log(spot_price / strike))
                approx_gamma = (1.0 / (iv * math.sqrt(t_days) * math.sqrt(2 * math.pi))) * math.exp(-0.5 * (distance / (iv * math.sqrt(t_days)))**2) / spot_price 

                d1 = (math.log(spot_price / strike) + (0.5 * (iv ** 2)) * t_days) / (iv * math.sqrt(t_days))
                d2 = d1 - iv * math.sqrt(t_days) 
                pdf_value = native_norm_pdf(d1)

                if option_type == 'C':
                    charm_per_contract = -pdf_value * ((0.0) / (iv * math.sqrt(t_days)) - d2 / (2 * t_days))
                else:
                    charm_per_contract = pdf_value * ((0.0) / (iv * math.sqrt(t_days)) + d2 / (2 * t_days)) 

                charm_day_footprint = charm_per_contract / 365.0 
                vanna_per_contract = -pdf_value * (d2 / iv)
                vanna_exposure_footprint = oi * vanna_per_contract * 0.01 * 100.0
                if option_type == 'P': vanna_exposure_footprint = -vanna_exposure_footprint
            except Exception:
                approx_gamma = 0.0001
                charm_day_footprint = 0.0
                vanna_exposure_footprint = 0.0 

            gex_value = oi * approx_gamma * (spot_price ** 2) * 0.01 * 100.0
            item_charm_exposure = oi * charm_day_footprint * 100.0
            if option_type == 'P':
                gex_value = -gex_value
                item_charm_exposure = -item_charm_exposure 

            net_charm_accumulator += item_charm_exposure 
            net_speed_current += calculate_speed_for_option(spot_price, strike, iv, days_to_expiry, oi, option_type)
            net_speed_down_10 += calculate_speed_for_option(spot_price - 5.0, strike, iv, days_to_expiry, oi, option_type)
            net_speed_up_10 += calculate_speed_for_option(spot_price + 5.0, strike, iv, days_to_expiry, oi, option_type) 

            parsed_options.append({
                'strike': strike, 'type': option_type, 'oi': oi, 'volume': volume,
                'gex': gex_value, 'vanna': vanna_exposure_footprint, 'iv': iv * 100.0, 'days_to_expiry': days_to_expiry
            }) 

    base_df = pd.DataFrame(parsed_options)
    if base_df.empty: return None 

    df_1m = base_df[base_df['days_to_expiry'] <= 30.0]
    df_3d = base_df[base_df['days_to_expiry'] <= 5.0]

    iv_shift_multiplier = 1.0
    if len(last_known_atm_iv) > 0:
        if atm_iv < last_known_atm_iv[-1]: iv_shift_multiplier = -1.0 
    last_known_atm_iv.append(atm_iv)
    if len(last_known_atm_iv) > 20: last_known_atm_iv.pop(0) 

    call_gex_1m = df_1m[df_1m['type'] == 'C']['gex'].sum()
    put_gex_1m = df_1m[df_1m['type'] == 'P']['gex'].sum()
    net_gex_1m = call_gex_1m + put_gex_1m
    total_abs_gex_1m = abs(call_gex_1m) + abs(put_gex_1m)
    call_weight_pct_1m = (abs(call_gex_1m) / total_abs_gex_1m * 100) if total_abs_gex_1m > 0 else 50.0 

    call_gex_3d = df_3d[df_3d['type'] == 'C']['gex'].sum()
    put_gex_3d = df_3d[df_3d['type'] == 'P']['gex'].sum()
    net_gex_3d = call_gex_3d + put_gex_3d
    total_abs_gex_3d = abs(call_gex_3d) + abs(put_gex_3d)
    call_weight_pct_3d = (abs(call_gex_3d) / total_abs_gex_3d * 100) if total_abs_gex_3d > 0 else 50.0

    center_strike = round(spot_price)
    target_strikes = sorted([s for s in base_df['strike'].unique() if abs(s - center_strike) <= 15])

    chart_matrix = []
    for idx, b_strike in enumerate(target_strikes):
        match_df = base_df[base_df['strike'] == b_strike]
        gex_3d_val = match_df[match_df['days_to_expiry'] <= 5.0]['gex'].sum()
        gex_1m_val = match_df[match_df['days_to_expiry'] <= 30.0]['gex'].sum()
        vanna_val = match_df['vanna'].sum()
        iv_skew_val = match_df['iv'].mean()
        b_vol = match_df['volume'].sum()
        b_oi = match_df['oi'].sum()
        velocity_pct = (b_vol / b_oi * 100.0) if b_oi > 0 else 0.0 

        chart_matrix.append({
            "index": idx, "strike": b_strike, "gex_3d": gex_3d_val, "abs_gex_3d": abs(gex_3d_val),
            "gex_1m": gex_1m_val, "abs_gex_1m": abs(gex_1m_val), "vanna_exposure": vanna_val,
            "vanna_flow": vanna_val * iv_shift_multiplier, "velocity_ratio": velocity_pct, "iv_skew": iv_skew_val
        }) 

    realized_vol_10d_val = calculate_realized_vol_10d(ticker) 

    return {
        "spot": spot_price, "call_gex_1m": call_gex_1m, "put_gex_1m": put_gex_1m, "net_gex_1m": net_gex_1m, "call_weight_1m": call_weight_pct_1m,
        "call_gex_3d": call_gex_3d, "put_gex_3d": put_gex_3d, "net_gex_3d": net_gex_3d, "call_weight_3d": call_weight_pct_3d,
        "max_pain": center_strike, "flip": center_strike - 2, "breakout": center_strike + 5, "resistance": center_strike + 3, "support": center_strike - 3,
        "call_inflow": call_gex_1m, "put_inflow": put_gex_1m, "net_flow": net_gex_1m, "chart_data": chart_matrix,
        "skew_25d": 0.0, "c1_wall": center_strike + 2, "c2_wall": center_strike + 4, "p1_wall": center_strike - 2, "p2_wall": center_strike - 4,
        "implied_vol": atm_iv, "realized_vol": realized_vol_10d_val, "trend_score": 6.0, "pt_gex": 1.5, "pt_flow": 1.5, "pt_price": 1.5, "pt_vol": 1.5,
        "net_charm_flow": net_charm_accumulator / 24.0, "ndf_drift_total": net_gex_3d,
        "aggr_call_ask": 0.0, "aggr_call_bid": 0.0, "aggr_put_ask": 0.0, "aggr_put_bid": 0.0,
        "speed_current": net_speed_current, "speed_down_1000": net_speed_down_10, "speed_up_1000": net_speed_up_10,
        "iv_direction": "EXPANDING" if iv_shift_multiplier > 0 else "CRUSHING"
    } 

def fmt_gex(val):
    return f"{val/1000000.0:+.2f}M"

def main(page: ft.Page):
    page.title = "SPY OPTIONS REAL-TIME GEX DASHBOARD"
    page.theme_mode = ft.ThemeMode.DARK
    page.scroll = ft.ScrollMode.AUTO
    page.padding = 14 

    net_axis_3d = ft.ChartAxis(labels=[], labels_size=24)
    abs_axis_3d = ft.ChartAxis(labels=[], labels_size=24)
    net_axis_1m = ft.ChartAxis(labels=[], labels_size=24)
    abs_axis_1m = ft.ChartAxis(labels=[], labels_size=24)
    vanna_bottom_axis = ft.ChartAxis(labels=[], labels_size=24)
    velocity_bottom_axis = ft.ChartAxis(labels=[], labels_size=24)
    iv_bottom_axis = ft.ChartAxis(labels=[], labels_size=24)
    iv_left_axis = ft.ChartAxis(labels=[], labels_size=42) 

    spot_price_container = ft.Text("$0.00", size=14, weight=ft.FontWeight.BOLD, color="#b5d045") 

    call_gex_txt_1m = ft.Text("0.0M", size=14, weight=ft.FontWeight.W_600)
    put_gex_txt_1m = ft.Text("0.0M", size=14, weight=ft.FontWeight.W_600)
    net_gex_txt_1m = ft.Text("0.0M", size=14, weight=ft.FontWeight.BOLD)
    weight_txt_1m = ft.Text("0.0%", size=14, weight=ft.FontWeight.W_600, color=ft.colors.BLUE_300) 

    call_gex_txt_3d = ft.Text("0.0M", size=14, weight=ft.FontWeight.W_600)
    put_gex_txt_3d = ft.Text("0.0M", size=14, weight=ft.FontWeight.W_600)
    net_gex_txt_3d = ft.Text("0.0M", size=14, weight=ft.FontWeight.BOLD)
    weight_txt_3d = ft.Text("0.0%", size=14, weight=ft.FontWeight.W_600, color="#ab47bc") 

    c1_txt = ft.Text("$0.00", size=14, weight=ft.FontWeight.BOLD, color=ft.colors.GREEN_400) 
    p1_txt = ft.Text("$0.00", size=14, weight=ft.FontWeight.BOLD, color=ft.colors.RED_400) 

    pain_txt = ft.Text("$0.00", size=14, weight=ft.FontWeight.W_600) 
    flip_txt = ft.Text("$0.00", size=14, weight=ft.FontWeight.W_600, color=ft.colors.ORANGE_400)
    breakout_txt = ft.Text("$0.00", size=14, weight=ft.FontWeight.W_600, color=ft.colors.GREEN_ACCENT) 

    iv_metric_txt = ft.Text("0.0%", size=14, weight=ft.FontWeight.W_600)
    rv_metric_txt = ft.Text("0.0%", size=14, weight=ft.FontWeight.W_600)

    grid_lines_config = ft.ChartGridLines(color=ft.colors.GREY_800, width=0.5)

    speed_curr_txt = ft.Text("0.00", size=14, weight=ft.FontWeight.W_600)
    speed_down_txt = ft.Text("0.00", size=14, weight=ft.FontWeight.W_600)
    speed_up_txt = ft.Text("0.00", size=14, weight=ft.FontWeight.W_600)

    gex_bar_chart_3d = ft.BarChart(bar_groups=[], bottom_axis=net_axis_3d, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    abs_gex_chart_3d = ft.BarChart(bar_groups=[], bottom_axis=abs_axis_3d, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    gex_bar_chart_1m = ft.BarChart(bar_groups=[], bottom_axis=net_axis_1m, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    abs_gex_chart_1m = ft.BarChart(bar_groups=[], bottom_axis=abs_axis_1m, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    vanna_bar_chart = ft.BarChart(bar_groups=[], bottom_axis=vanna_bottom_axis, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    velocity_bar_chart = ft.BarChart(bar_groups=[], bottom_axis=velocity_bottom_axis, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 
    id_skew_bar_chart = ft.BarChart(bar_groups=[], bottom_axis=iv_bottom_axis, left_axis=iv_left_axis, horizontal_grid_lines=grid_lines_config, vertical_grid_lines=grid_lines_config, animate=True, height=240) 

    def create_section_header(title):
        return ft.Container(content=ft.Text(title, size=13, weight=ft.FontWeight.BOLD, color=ft.colors.GREY_500), margin=ft.margin.only(top=15, bottom=5)) 

    def ui_row_item(label, component):
        return ft.Container(content=ft.Row([ft.Text(label, size=14, color=ft.colors.GREY_300), component], alignment=ft.MainAxisAlignment.SPACE_BETWEEN), padding=ft.padding.symmetric(vertical=4)) 

    def refresh_dashboard():
        m = fetch_deribit_gex("SPY")
        if m:
            spot_price_container.value = f"${m['spot']:,.2f}" 
            call_gex_txt_1m.value = fmt_gex(m['call_gex_1m'])
            put_gex_txt_1m.value = fmt_gex(m['put_gex_1m'])
            net_gex_txt_1m.value = fmt_gex(m['net_gex_1m'])
            weight_txt_1m.value = f"{m['call_weight_1m']:.1f}%" 

            call_gex_txt_3d.value = fmt_gex(m['call_gex_3d'])
            put_gex_txt_3d.value = fmt_gex(m['put_gex_3d'])
            net_gex_txt_3d.value = fmt_gex(m['net_gex_3d'])
            weight_txt_3d.value = f"{m['call_weight_3d']:.1f}%" 

            c1_txt.value = f"${m['c1_wall']:.1f}"
            p1_txt.value = f"${m['p1_wall']:.1f}" 

            pain_txt.value = f"${m['max_pain']:.1f}"
            flip_txt.value = f"${m['flip']:.1f}"
            breakout_txt.value = f"${m['breakout']:.1f}" 

            iv_val, rv_val = m['implied_vol'], m['realized_vol']
            iv_metric_txt.value = f"{iv_val:.1f}%"
            rv_metric_txt.value = f"{rv_val:.1f}%" 

            sp_curr, sp_down, sp_up = m['speed_current'], m['speed_down_1000'], m['speed_up_1000']
            speed_curr_txt.value = f"{sp_curr:+.4f}"
            speed_down_txt.value = f"{sp_down:+.4f}"
            speed_up_txt.value = f"{sp_up:+.4f}" 

            # --- CLEANED INDEPENDENT INITIALIZATION PER LINE TO AVOID TUPLE SIZE VALUEERROR ---
            groups_net_3d = []
            groups_abs_3d = []
            groups_net_1m = []
            groups_abs_1m = []
            groups_vanna = []
            groups_velocity = []
            iv_bar_groups = []
            new_labels = []

            for item in m['chart_data']:
                strike_val = item['strike']
                idx = item['index']
                groups_net_3d.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['gex_3d']/100000.0, color=ft.colors.GREEN_400 if item['gex_3d'] >= 0 else ft.colors.RED_400, width=12)]))
                groups_abs_3d.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['abs_gex_3d']/100000.0, color=ft.colors.YELLOW, width=12)]))
                groups_net_1m.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['gex_1m']/100000.0, color=ft.colors.BLUE_400, width=12)]))
                groups_abs_1m.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['abs_gex_1m']/100000.0, color=ft.colors.PURPLE, width=12)]))
                groups_vanna.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['vanna_exposure']/100000.0, color=ft.colors.ORANGE, width=12)]))
                groups_velocity.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['velocity_ratio'], color=ft.colors.CYAN, width=12)]))
                iv_bar_groups.append(ft.BarChartGroup(x=idx, bar_rods=[ft.BarChartRod(from_y=0, to_y=item['iv_skew'], color=ft.colors.RED_ACCENT, width=12)]))
                
                if idx % 3 == 0:
                    new_labels.append(ft.ChartAxisLabel(value=idx, label=ft.Text(f"{int(strike_val)}", size=10, rotate=45)))

            gex_bar_chart_3d.bar_groups = groups_net_3d
            net_axis_3d.labels = new_labels
            abs_gex_chart_3d.bar_groups = groups_abs_3d
            abs_axis_3d.labels = new_labels
            gex_bar_chart_1m.bar_groups = groups_net_1m
            net_axis_1m.labels = new_labels
            abs_gex_chart_1m.bar_groups = groups_abs_1m
            abs_axis_1m.labels = new_labels
            vanna_bar_chart.bar_groups = groups_vanna
            vanna_bottom_axis.labels = new_labels
            velocity_bar_chart.bar_groups = groups_velocity
            velocity_bottom_axis.labels = new_labels
            id_skew_bar_chart.bar_groups = iv_bar_groups
            iv_bottom_axis.labels = new_labels
            
            page.update()

    page.add(
        ft.Row([ft.Text("SPY GEX DASHBOARD", size=20, weight=ft.FontWeight.BOLD)], alignment=ft.MainAxisAlignment.START),
        ft.Card(content=ft.Container(content=ft.Row([ft.Text("SPY ETF Spot Price", size=11, color=ft.colors.GREY_500), spot_price_container], alignment=ft.MainAxisAlignment.SPACE_BETWEEN), padding=12)),
        
        create_section_header("NET GAMMA EXPOSURE BY STRIKE (SHORT-TERM)"),
        ft.Card(content=ft.Container(padding=15, content=gex_bar_chart_3d)),
        
        create_section_header("ABS GAMMA EXPOSURE BY STRIKE (SHORT-TERM)"),
        ft.Card(content=ft.Container(padding=15, content=ft.Column([
            abs_gex_chart_3d, ft.Container(height=10),
            ui_row_item("Call Concentration (C1)", c1_txt), ui_row_item("Put Concentration (P1)", p1_txt)
        ]))),

        create_section_header("CRITICAL REHEDGING BOUNDARIES"),
        ft.Card(content=ft.Container(padding=14, content=ft.Column([ui_row_item("Estimated Inflection Strike", pain_txt), ui_row_item("Flip Zone", flip_txt), ui_row_item("Breakout Strike", breakout_txt)]))),

        create_section_header("NET GAMMA EXPOSURE BY STRIKE (30D MONTHLY)"),
        ft.Card(content=ft.Container(padding=15, content=gex_bar_chart_1m)),
        
        create_section_header("INTRADAY TURNOVER PROFILE (VOLUME / OI)"),
        ft.Card(content=ft.Container(padding=15, content=velocity_bar_chart)),

        create_section_header("NET VANNA PROFILE (VEX)"),
        ft.Card(content=ft.Container(padding=15, content=vanna_bar_chart)),

        create_section_header("TOTAL ACCUMULATED GEX METRICS"),
        ft.Card(content=ft.Container(padding=14, content=ft.Column([
            ui_row_item("Net Call Exposure", call_gex_txt_1m), ui_row_item("Net Put Exposure", put_gex_txt_1m), ui_row_item("Net Portfolio Balance", net_gex_txt_1m)
        ]))),
        
        create_section_header("VOLATILITY STRUCTURE MAPPING"),
        ft.Card(content=ft.Container(padding=14, content=ft.Column([
            ui_row_item("ATM Implied Volatility (IV)", iv_metric_txt), ui_row_item("10D Historical Volatility (RV)", rv_metric_txt)
        ]))),

        create_section_header("DEALER PROFILE ACCELERATION COEFFICIENTS (SPEED)"),
        ft.Card(content=ft.Container(padding=14, content=ft.Column([
            ui_row_item("Spot Speed Engine", speed_curr_txt), ui_row_item("Downside Slippage Profile (-$5)", speed_down_txt), ui_row_item("Upside Expansion Profile (+$5)", speed_up_txt)
        ])))
    )
    refresh_dashboard()

if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_data_worker, daemon=True)
    worker_thread.start()
    ft.app(target=main, port=int(os.environ.get("PORT", 8080)), host="0.0.0.0", view=None)
