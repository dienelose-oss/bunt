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
        'profit_preserve_on': False,      # 🚨 이익 보존 ON/OFF
        'profit_preserve_amount': 0,     # 🚨 고정 보존 금액
        'profit_trailing_on': False,      # 🚨 비율 추적 ON/OFF
        'profit_trailing_pct': 20.0,      # 🚨 최고점 대비 하락 비율(%)
        'protected_codes': [],            # 🚨 보호 종목 리스트
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

# ---------------------------------------------------------
# 5. 핵심 자동매매 감시 및 제어 루프 (신규 작성)
# ---------------------------------------------------------
async def main_loop():
    print("🚀 수석 엔지니어 버전 자동매매 시스템을 구동합니다...")
    
    # Redis 클라이언트 초기화 (실패 시 로컬 json 파일 사용)
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    async with aiohttp.ClientSession() as session:
        # 1. 설정 및 상태 로드
        settings = await load_or_init_settings(session)
        watch_list, alerted_obs, use_redis = await load_persistent_state(redis_client)
        
        # 텔레그램 봇 초기 알림
        await send_tg_message(f"✅ Bunt 봇 구동 시작.\n설정 베이스 자산: {settings['base_amount']:,}원")

        # 2. 웹소켓(틱 시세) 스트리밍 연결
        ws_manager = kiwoom_api.KISWebSocket(session)
        await ws_manager.connect()

        # 무한 루프 시작
        while True:
            try:
                now = datetime.now(KST)
                current_time_int = now.hour * 100 + now.minute
                today_str = now.strftime('%Y%m%d')

                # 장중 감시 시간 (09:00 ~ 15:20)
                is_market_open = (900 <= current_time_int < 1520)

                # =========================================================
                # 🌟 [글로벌 추세 필터] 코스피/코스닥 1분봉 5선 > 20선 정배열 확인
                # =========================================================
                if strategy.global_market_filter.needs_update(interval_seconds=60):
                    # API 한도 방지를 위해 1분에 1번만 지수 데이터를 호출합니다.
                    kospi_data = await kiwoom_api.get_index_minute_data(session, '0001')
                    kosdaq_data = await kiwoom_api.get_index_minute_data(session, '1001')
                    
                    is_trend_good = strategy.global_market_filter.update_trend(kospi_data, kosdaq_data)
                    
                    # 콘솔 로깅 (텔레그램 알림은 너무 잦으므로 생략 또는 옵션화 권장)
                    trend_status = "🟢 정배열 (안전)" if is_trend_good else "🔴 역배열 (매수 차단)"
                    print(f"📊 [시장 추세 {now.strftime('%H:%M:%S')}] 코스피/코스닥 상태: {trend_status}")

                if is_market_open:
                    # 현재 보유 종목 로드
                    holdings = await kiwoom_api.get_holdings_data(session) or {}
                    
                    # 글로벌 필터 판별 상태 (True/False)
                    can_buy_market = strategy.global_market_filter.is_safe_to_buy()
                    
                    # =========================================================
                    # 🔍 [매수 감시 로직] 감시 리스트(watch_list) 순회
                    # =========================================================
                    for code, info in list(watch_list.items()):
                        # 1) 이미 보유 중인 종목은 신규 매수 감시 패스
                        if code in holdings:
                            continue
                            
                        # 2) 시장 역배열 상태 방어막 (Circuit Breaker)
                        if not can_buy_market:
                            # 추세가 안 좋을 때는 매수 타점이 오더라도 진입을 포기합니다.
                            continue

                        # 3) 분봉 캔들 조회
                        candles = await kiwoom_api.get_candles(session, code, 1)
                        if not candles:
                            continue

                        # 4) 전략 타점 검사 (Gemini 엔진 예시)
                        if settings.get('engine_gem_on', True):
                            is_signal, meta = strategy.check_gemini_momentum_model(
                                candles, 
                                today_str, 
                                tp_pct=settings['gemini_tp_pct'], 
                                sl_pct=settings['gemini_sl_pct'], 
                                filter_lvl=settings['gemini_filter_lvl']
                            )
                            
                            if is_signal:
                                entry_price = meta['entry_price']
                                # 리스크 베이스 비중 계산
                                calc_qty = int(settings['gemini_amount'] // entry_price) if entry_price > 0 else 0
                                
                                if calc_qty > 0:
                                    # 실제 주문 API 호출
                                    msg, odno = await kiwoom_api.buy_limit_order(session, code, calc_qty, entry_price)
                                    
                                    # 결과 브리핑
                                    report_msg = (
                                        f"🚨 [자동 매수 시그널 발생]\n"
                                        f"• 종목코드: {code}\n"
                                        f"• 결과: {msg}\n"
                                        f"• 적용엔진: {meta['strategy']}\n"
                                        f"• 진단: {meta['meta']['diag_msg']}"
                                    )
                                    await send_tg_message(report_msg)
                                    
                                    # 매수 후 감시 리스트에서 해당 종목 삭제
                                    del watch_list[code]
                                    await save_watch_list(redis_client, watch_list, use_redis)

                    # =========================================================
                    # 💰 [매도 및 청산 로직] 잔고 스캔 (추후 고도화 필요 영역)
                    # =========================================================
                    # 보유 종목에 대한 TP(익절), SL(손절) 로직이 이곳에 추가되어야 합니다.
                    # 현재는 구조만 잡아두었습니다.
                    pass

                # API 호출 한도(Rate Limit) 방지용 대기
                await asyncio.sleep(1.0)

            except Exception as e:
                print(f"❌ [메인 루프 에러]: {e}")
                await asyncio.sleep(5) # 에러 발생 시 잠시 휴식

if __name__ == "__main__":
    try:
        # 최상위 비동기 이벤트 루프 실행
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 자동매매 시스템이 안전하게 종료되었습니다.")