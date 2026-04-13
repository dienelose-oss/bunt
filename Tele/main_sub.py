import os
import time
import csv
import json
import asyncio
import aiohttp
import re
import ast
import shutil
import pandas as pd
import redis.asyncio as redis
from datetime import datetime, time as dt_time, timezone, timedelta 
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import threading 

import kiwoom_api
import telegram_bot
import strategy

# ---------------------------------------------------------
# 1. 전역 상수 및 설정 파일 경로
# ---------------------------------------------------------
ASSET_FILE = 'asset_history.csv'
SETTINGS_FILE = 'settings.json'
STATE_FILE = 'watch_list_state.json'
ALERTED_OBS_FILE = 'alerted_obs.json'
REDIS_WATCH_KEY = 'auto_watch_list'
SNAPSHOT_FILE = 'd_minus_2_balance.json'
HOLIDAY_FILE = 'krx_holidays.json'

chart_lock = threading.Lock()
KST = timezone(timedelta(hours=9))

# 코스피/코스닥 소속 시장 캐싱 딕셔너리
_STOCK_MARKET_CACHE = {}


# ---------------------------------------------------------
# 2. 유틸리티 함수 (날짜, 시간, 틱 사이즈 등)
# ---------------------------------------------------------
def _get_tick_size(price):
    if price < 2000: return 1
    elif price < 5000: return 5
    elif price < 20000: return 10
    elif price < 50000: return 50
    elif price < 200000: return 100
    elif price < 500000: return 500
    else: return 1000

def get_d_minus_2_date(current_date_str):
    dt = datetime.strptime(current_date_str, '%Y%m%d')
    days_subtracted = 0
    while days_subtracted < 2:
        dt -= timedelta(days=1)
        if dt.weekday() < 5: 
            days_subtracted += 1
    return dt.strftime('%Y%m%d')

def get_remaining_trading_days(start_date_str, target_date_str):
    try:
        start_dt = datetime.strptime(start_date_str, '%Y%m%d')
        target_dt = datetime.strptime(target_date_str, '%Y-%m-%d')
        
        holidays = []
        if os.path.exists(HOLIDAY_FILE):
            with open(HOLIDAY_FILE, 'r', encoding='utf-8') as f:
                holidays = json.load(f)
                
        days = 0
        curr_dt = start_dt
        while curr_dt < target_dt:
            curr_dt += timedelta(days=1)
            if curr_dt.weekday() < 5: 
                if curr_dt.strftime('%Y-%m-%d') not in holidays:
                    days += 1
        return days
    except Exception:
        return 0

def get_time_session(ts):
    dt = datetime.fromtimestamp(ts, tz=KST)
    hm = dt.hour * 100 + dt.minute
    if hm < 930: return 'Open(09:00~09:30)'
    elif hm < 1400: return 'Mid(09:30~14:00)'
    else: return 'Close(14:00~)'


# ---------------------------------------------------------
# 3. 로컬 파일 및 상태 관리 함수 (저장/불러오기)
# ---------------------------------------------------------
async def load_persistent_state(redis_client):
    watch_list = {}
    use_redis = False
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=1.0)
        data = await redis_client.hgetall(REDIS_WATCH_KEY)
        watch_list = {k: json.loads(v) for k, v in data.items()}
        use_redis = True
    except Exception:
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    watch_list = json.load(f)
            except Exception:
                pass
            
    alerted_obs = set()
    if os.path.exists(ALERTED_OBS_FILE):
        try:
            with open(ALERTED_OBS_FILE, 'r') as f:
                alerted_obs = set(json.load(f))
        except Exception:
            pass
        
    return watch_list, alerted_obs, use_redis

async def save_watch_list(redis_client, watch_list, use_redis):
    if use_redis:
        try:
            await redis_client.delete(REDIS_WATCH_KEY)
            if watch_list:
                mapping = {k: json.dumps(v) for k, v in watch_list.items()}
                await redis_client.hset(REDIS_WATCH_KEY, mapping=mapping)
        except Exception as e:
            print(f"Redis 백업 실패: {e}")
    else:
        def _write():
            with open(STATE_FILE, 'w') as f:
                json.dump(watch_list, f, indent=4)
        await asyncio.to_thread(_write)

async def save_alerted_obs(alerted_obs_set):
    def _write():
        with open(ALERTED_OBS_FILE, 'w') as f:
            json.dump(list(alerted_obs_set), f)
    await asyncio.to_thread(_write)

async def load_or_init_settings(session):
    default_settings = {
        'base_amount': 0,
        'risk_amount': 500000, 
        'auto_rr_ratio': 2.0, 
        'gemini_amount': 500000,
        'gemini_tp_pct': 1.5,
        'gemini_sl_pct': 1.0,
        'gemini_filter_lvl': 2,
        'gemini_pullback_pct': 1.0,
        'rvol_amount': 500000,
        'rvol_filter_lvl': 2,
        'engine_rvol_on': True,
        'cancel_timeout_mins': 30,
        'buy_yield_ticks': 3,
        'nxt_scan_enabled': False,
        'keep_tracking_today': True,
        'max_tracking_items': 30,
        'custom_watchlist': {},
        'engine_gem_on': True,
        'engine_laptop_on': True, 
        'time_filter_on': False,
        'auto_remove_unheld': False,
        'target_date': '2026-12-20',
        'target_amount': 100000000,
        'planner_rate_type': '복리',
        'planner_sell_target': 'auto',
        'auto_shutdown_on_target': False,
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                loaded = json.load(f)
                if 'custom_watchlist' in loaded and isinstance(loaded['custom_watchlist'], list):
                    loaded['custom_watchlist'] = {code: f"관심종목({code})" for code in loaded['custom_watchlist']}
                default_settings.update(loaded)
                return default_settings
        except Exception:
            pass
            
    print("초기 설정 파일 생성 중. API로 잔고를 확인합니다...")
    while True:
        assets = await kiwoom_api.get_estimated_assets(session)
        if assets is not None:
            default_settings['base_amount'] = assets
            break
        await asyncio.sleep(3)
        
    def _save():
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(default_settings, f, indent=4)
    await asyncio.to_thread(_save)
    return default_settings

def write_trade_log(filename, headers, row_data):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        writer.writerow(row_data)


# ---------------------------------------------------------
# 4. 외부 API 크롤링 및 시각화 함수
# ---------------------------------------------------------
async def send_tg_message(msg, reply_markup=None):
    await asyncio.to_thread(telegram_bot.send_message, msg, reply_markup=reply_markup)

async def get_stock_info_from_naver(session, code):
    url = f"https://m.stock.naver.com/api/stock/{code}/basic"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'
    }
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            data = await resp.json()
            if data and 'stockName' in data:
                name = data['stockName'].strip()
                mkt_str = data.get('stockExchangeType', {}).get('name', '').upper()
                market = 'KOSPI' if 'KOSPI' in mkt_str else 'KOSDAQ'
                return name, market
    except Exception as e:
        print(f"네이버 모바일 API 종목명 역산 오류: {e}")
    return None, 'KOSDAQ'

async def get_kosdaq_macro_trend(session):
    url = "https://m.stock.naver.com/api/index/KOSDAQ/basic"
    headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/80.0.3987.162 Mobile Safari/537.36'}
    try:
        async with session.get(url, headers=headers, timeout=3) as resp:
            data = await resp.json()
            return float(data.get('fluctuationsRatio', 0.0))
    except Exception:
        return 0.0

async def get_macro_minute_candles(session, market):
    now = datetime.now(KST)
    start_time = (now - timedelta(days=1)).strftime('%Y%m%d') + "090000"
    end_time = now.strftime('%Y%m%d%H%M%S')
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={market}&requestType=1&startTime={start_time}&endTime={end_time}&timeframe=minute"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            text = await resp.text()
            text = text.replace("'", '"').replace('\n', '').strip()
            matches = re.findall(r'\["(\d{12})",\s*([\d\.]+),\s*([\d\.]+),\s*([\d\.]+),\s*([\d\.]+),\s*(\d+)\]', text)
            candles = []
            for m in matches[-20:]: 
                candles.append({
                    'time': m[0],
                    'close': float(m[4])
                })
            candles.reverse() 
            return candles
    except Exception as e:
        print(f"매크로 지수 수집 오류 [{market}]: {e}")
        return []

def load_today_asset_data():
    history = []
    max_amt = 0
    max_tm = ""
    today_str = datetime.now(KST).strftime('%Y-%m-%d')
    if os.path.exists(ASSET_FILE):
        try:
            with open(ASSET_FILE, 'r') as f:
                for row in csv.reader(f):
                    if len(row) == 2 and row[0].startswith(today_str):
                        dt_obj = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').replace(tzinfo=KST)
                        if dt_time(8, 0) <= dt_obj.time() <= dt_time(20, 0):
                            amt = int(row[1])
                            history.append((dt_obj, amt))
                            if amt > max_amt: 
                                max_amt = amt
                                max_tm = dt_obj.strftime('%H:%M:%S')
        except Exception:
            pass
    return history, max_amt, max_tm

def _generate_and_send_asset_chart(asset_history, base_amount, max_assets_today, max_assets_time):
    if len(asset_history) < 2: return False
    with chart_lock:
        times = [x[0] for x in asset_history]
        assets = [x[1] for x in asset_history]
        plt.figure(figsize=(10, 5))
        line_color = 'red' if assets[-1] >= base_amount else 'blue'
        plt.plot(times, assets, color=line_color, linewidth=2)
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False
        plt.title("당일 실시간 추정자산 흐름 (08:00 ~ 20:00)")
        plt.grid(True, linestyle='--', alpha=0.6)
        
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=KST))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        chart_path = 'asset_chart.png'
        
        try:
            plt.savefig(chart_path)
        except Exception as e:
            print(f"차트 저장 실패 (권한 문제 등): {e}")
        finally:
            plt.close()
        
    diff = assets[-1] - base_amount
    diff_str = f"+{diff:,}" if diff > 0 else f"{diff:,}"
    caption = f"📊 현재 자산: {assets[-1]:,}원 ({diff_str}원)"
    if max_assets_today > 0:
        max_diff = max_assets_today - base_amount
        caption += f"\n👑 최고 자산: {max_assets_today:,}원 ({f'+{max_diff:,}' if max_diff>0 else f'{max_diff:,}'}원) - {max_assets_time} 기준"
    telegram_bot.send_photo(chart_path, caption=caption)
    return True