import os
import time
import csv
import json
import asyncio
import aiohttp
import re
import ast
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

async def send_tg_message(msg, reply_markup=None):
    await asyncio.to_thread(telegram_bot.send_message, msg, reply_markup=reply_markup)

async def load_or_init_settings(session):
    default_settings = {
        'base_amount': 0,
        'risk_amount': 500000, 
        'auto_rr_ratio': 2.0, 
        'gemini_amount': 500000,
        'gemini_tp_pct': 1.5,
        'gemini_sl_pct': 1.0,
        'gemini_filter_lvl': 3,
        'nxt_scan_enabled': False,
        'custom_watchlist': {},
        'engine_gem_on': True,
        'engine_laptop_on': True, 
        'time_filter_on': False,
        'auto_remove_unheld': False,
        'target_date': '2026-12-20',
        'target_amount': 100000000,
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

# 종목 이름 및 코스피/코스닥 소속 시장 판별 함수
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

# 🚨 코스피/코스닥 실시간 지수 1분봉 20개 수집 함수 (이동평균 연산용)
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
            candles.reverse() # KIS API 규격에 맞춰 최신 데이터를 인덱스 0으로 뒤집기
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
        
        # 🚨 한국 시간(KST)으로 그래프 하단 라벨 포맷 고정
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

def write_trade_log(filename, headers, row_data):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        writer.writerow(row_data)

# 🚨 타임 세션 판별 유틸리티 추가
def get_time_session(ts):
    dt = datetime.fromtimestamp(ts, tz=KST)
    hm = dt.hour * 100 + dt.minute
    if hm < 930: return 'Open(09:00~09:30)'
    elif hm < 1400: return 'Mid(09:30~14:00)'
    else: return 'Close(14:00~)'

async def main():
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auto_watch_list, alerted_obs, use_redis = await load_persistent_state(redis_client)
    api_semaphore = asyncio.Semaphore(4)

    stock_dict = {}
    pending_setups = {} 
    
    last_monitor_time = 0
    last_scan_time = 0
    last_asset_record_time = 0
    last_auto_chart_time = 0
    last_sync_time = 0
    
    last_cleared_hour = None
    last_daily_reset_date = None
    last_snapshot_date = None 
    awaiting_setting = None
    
    is_paused = False
    current_macro_pct = 0.0 
    
    last_engine_scan_time = "스캔 대기 중"
    
    asset_history, max_assets_today, max_assets_time = await asyncio.to_thread(load_today_asset_data)

    async with aiohttp.ClientSession() as session:
        user_settings = await load_or_init_settings(session)
        print(f"\n✅ 시스템 초기화 완료. 백업 관제 {len(auto_watch_list)}개, 알림 로그 {len(alerted_obs)}개 복구.")
        
        kiwoom_api.ws_client = kiwoom_api.KISWebSocket(session)
        await kiwoom_api.ws_client.connect()
        for code in auto_watch_list.keys():
            await kiwoom_api.ws_client.subscribe(code)

        welcome_msg = f"🤖 [자동매매 시스템 가동 시작]\n초기화를 완료했습니다. 아래 대시보드에서 시스템을 통제하십시오."
        dash_reply_markup = {
            "inline_keyboard": [
                [{"text": "📈 관심종목 관리", "callback_data": "menu_watch"}, {"text": "👀 감시 현황", "callback_data": "menu_monitor"}],
                [{"text": "💰 계좌 잔고", "callback_data": "menu_balance"}, {"text": "📊 누적 통계 분석", "callback_data": "menu_analysis"}],
                [{"text": "⚙️ 엔진 세팅", "callback_data": "menu_setting"}, {"text": "💡 도움말", "callback_data": "menu_help"}],
                [{"text": "💸 미수/반대매매 방어 (D-2)", "callback_data": "menu_margin_clear"}],
                [{"text": "🎯 목표 달성 플래너", "callback_data": "menu_planner"}]
            ]
        }
        await send_tg_message(welcome_msg, reply_markup=dash_reply_markup)

        # ---------------------------------------------------------------------
        # [비동기 멀티 태스킹 개편] 기능별 4개 태스크 분리
        # ---------------------------------------------------------------------

        # 1. 텔레그램 명령어 처리 태스크
        async def task_telegram():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date
            
            while True:
                now = datetime.now(KST) 
                today_str = now.strftime('%Y%m%d')
                new_commands = await asyncio.to_thread(telegram_bot.fetch_commands)
                
                for cmd in new_commands:
                    if cmd == 'ㄱ':
                        if len(asset_history) >= 2:
                            await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                        else:
                            await send_tg_message("⚠️ 아직 자산 차트를 그릴 충분한 데이터가 수집되지 않았습니다 (최소 20분 소요).")
                        continue

                    if cmd == 'ㅁ':
                        msg = f"🎛️ [메인 관제탑 대시보드]\n"
                        msg += f"📡 최근 엔진 스캔: {last_engine_scan_time}\n\n"
                        msg += "원하시는 메뉴를 터치하십시오."
                        reply_markup = {
                            "inline_keyboard": [
                                [{"text": "📈 관심종목 관리", "callback_data": "menu_watch"}, {"text": "👀 감시 현황", "callback_data": "menu_monitor"}],
                                [{"text": "💰 계좌 잔고", "callback_data": "menu_balance"}, {"text": "📊 누적 통계 분석", "callback_data": "menu_analysis"}],
                                [{"text": "⚙️ 엔진 세팅", "callback_data": "menu_setting"}, {"text": "💡 도움말", "callback_data": "menu_help"}],
                                [{"text": "💸 미수/반대매매 방어 (D-2)", "callback_data": "menu_margin_clear"}],
                                [{"text": "🎯 목표 달성 플래너", "callback_data": "menu_planner"}]
                            ]
                        }
                        await send_tg_message(msg, reply_markup=reply_markup)
                        continue

                    if cmd.startswith('cb:'):
                        cb_data = cmd.replace('cb:', '')
                        if cb_data == 'menu_setting': cmd = '세팅'
                        elif cb_data == 'menu_monitor': cmd = '감시'
                        elif cb_data == 'menu_balance': cmd = '잔고'
                        elif cb_data == 'menu_analysis': cmd = '분석'
                        elif cb_data == 'menu_help':
                            help_msg = """💡 **[시스템 도움말 가이드]**\n\n단축키 기능:\n채팅창에 아래의 글자를 입력하고 전송하세요.\n• **'ㅁ'** : 메인 관제탑 대시보드를 언제든 즉시 호출합니다.\n• **'ㄱ'** : 당일 실시간 자산 흐름 그래프를 렌더링하여 보여줍니다.\n\n작동 중인 자동매매 엔진:\n• **🤖 제미나이(스캘핑)** : 거래량이 폭발하는 양봉을 포착해 빠른 목표가에 전량 자동 익/손절합니다. (설정된 금액 고정 매수)\n• **💻 랩탑(스윙)** : 5분봉 기준 VWAP 상단의 눌림목 추세를 따라 자동 진입합니다. (리스크 금액 기반 수량 산출)\n\n기본 백그라운드 기능:\n• 10분 주기 자산 자동 보고 및 그래프 전송\n• 15시 20분 D-2 예수금 및 종가 보유 상태 자동 기록 (미수 방어용)"""
                            await send_tg_message(help_msg)
                            continue
                        
                        elif cb_data == 'menu_planner':
                            current_assets = await kiwoom_api.get_estimated_assets(session)
                            if current_assets is None: current_assets = user_settings.get('base_amount', 0)
                            
                            target_date = user_settings.get('target_date', '2026-12-20')
                            target_amt = user_settings.get('target_amount', 100000000)
                            rem_days = get_remaining_trading_days(today_str, target_date)
                            
                            needed_amt = target_amt - current_assets
                            
                            msg = f"🎯 [목표 달성 플래너]\n\n"
                            msg += f"• 목표일: {target_date} (남은 영업일: {rem_days}일)\n"
                            msg += f"• 목표 금액: {target_amt:,}원\n"
                            msg += f"• 현재 자산: {current_assets:,}원\n"
                            
                            if needed_amt <= 0:
                                msg += "\n🎉 **목표 금액을 이미 달성했습니다!** 축하합니다!"
                            elif rem_days <= 0:
                                msg += "\n⚠️ 목표일이 지났거나 오늘입니다. 목표일을 연장해주세요."
                            else:
                                daily_req = needed_amt / rem_days
                                daily_pct = (daily_req / current_assets) * 100 if current_assets > 0 else 0
                                msg += f"• 남은 금액: {needed_amt:,}원\n\n"
                                msg += f"🔥 **하루 목표 수익금**: {int(daily_req):,}원\n"
                                msg += f"📈 **현 자산 대비 필요 수익률**: 하루 {daily_pct:.2f}%\n"
                                
                            reply_markup = {
                                "inline_keyboard": [
                                    [{"text": "🗓️ 목표일 변경", "callback_data": "set_target_date"}, {"text": "💰 목표금액 변경", "callback_data": "set_target_amount"}]
                                ]
                            }
                            await send_tg_message(msg, reply_markup=reply_markup)
                            continue

                        elif cb_data == 'sync_holdings':
                            holdings = await kiwoom_api.get_holdings_data(session)
                            if holdings is None:
                                await send_tg_message("❌ 잔고 데이터를 수신하지 못했습니다. API 연결 상태를 확인하세요.")
                            else:
                                removed = 0
                                for code in list(auto_watch_list.keys()):
                                    if code not in holdings or holdings[code].get('qty', 0) == 0:
                                        del auto_watch_list[code]
                                        if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                        removed += 1
                                if removed > 0:
                                    await save_watch_list(redis_client, auto_watch_list, use_redis)
                                    await send_tg_message(f"✅ 동기화 완료! 실제 잔고에 없는 {removed}개 종목을 감시 목록에서 삭제했습니다.")
                                else:
                                    await send_tg_message("✅ 감시 목록과 실제 계좌 잔고가 완벽히 일치합니다.")
                            continue
                            
                        elif cb_data == 'menu_watch':
                            watchlist = user_settings.get('custom_watchlist', {})
                            msg = f"📈 [관심종목 관리] (현재 {len(watchlist)}/10개)\n\n"
                            if watchlist:
                                msg += "현재 등록된 종목:\n" + "\n".join([f"• {name} ({code})" for code, name in watchlist.items()])
                            else:
                                msg += "등록된 종목이 없습니다."
                            reply_markup = {
                                "inline_keyboard": [
                                    [{"text": "➕ 종목 추가", "callback_data": "watch_add"}, {"text": "➖ 종목 삭제", "callback_data": "watch_del_menu"}]
                                ]
                            }
                            await send_tg_message(msg, reply_markup=reply_markup)
                            continue
                        elif cb_data == 'watch_add':
                            awaiting_setting = 'watch_add'
                            await send_tg_message("✏️ 추가할 관심종목 코드(6자리 숫자)를 채팅창에 입력하세요:")
                            continue
                        elif cb_data == 'watch_del_menu':
                            watchlist = user_settings.get('custom_watchlist', {})
                            if not watchlist:
                                await send_tg_message("❌ 등록된 관심종목이 없습니다.")
                                continue
                            keyboard = []
                            for code, name in watchlist.items():
                                keyboard.append([{"text": f"❌ {name} ({code}) 삭제", "callback_data": f"delwatchitem_{code}"}])
                            await send_tg_message("🗑️ 삭제할 종목을 터치하세요:", reply_markup={"inline_keyboard": keyboard})
                            continue
                        elif cb_data.startswith('delwatchitem_'):
                            code = cb_data.split('_')[1]
                            watchlist = user_settings.get('custom_watchlist', {})
                            if code in watchlist:
                                name = watchlist.pop(code)
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                await send_tg_message(f"🗑️ [{name}] 관심종목에서 영구 삭제되었습니다.")
                            continue
                        
                        elif cb_data == 'menu_margin_clear':
                            bal_text = await kiwoom_api.get_account_balance(session)
                            if not bal_text: bal_text = ""
                            
                            deposit_match = re.search(r'예수금\s*[:=]?\s*([0-9,]+)', bal_text)
                            deposit_str = deposit_match.group(1) + "원" if deposit_match else "잔고 메뉴에서 직접 확인 요망"

                            d_minus_2 = get_d_minus_2_date(today_str)
                            
                            snap_data = {}
                            if os.path.exists(SNAPSHOT_FILE):
                                try:
                                    with open(SNAPSHOT_FILE, 'r') as f:
                                        snap_data = json.load(f)
                                except: pass
                                
                            target_holdings = snap_data.get(d_minus_2, {})
                            
                            msg = f"🚨 [미수/반대매매 방어 진단]\n"
                            msg += f"• 당일 예수금: {deposit_str}\n\n"
                            msg += f"📊 [D-2 ({d_minus_2}) 15:20 기준 평가금액 리스트]\n"
                            
                            if not target_holdings:
                                msg += "❌ 해당 일자(D-2) 15시 20분의 잔고 스냅샷 기록이 존재하지 않습니다."
                            else:
                                sortable_list = []
                                for h_code, h_data in target_holdings.items():
                                    qty = h_data.get('qty', 0)
                                    eval_amt = h_data.get('eval_amt', h_data.get('purchase_price', 0) * qty)
                                    sortable_list.append({'name': h_data.get('name', h_code), 'qty': qty, 'eval_amt': eval_amt})
                                    
                                sortable_list.sort(key=lambda x: x['eval_amt'], reverse=True)
                                
                                for i, item in enumerate(sortable_list, 1):
                                    msg += f"{i}. {item['name']}: {int(item['eval_amt']):,}원 ({item['qty']}주)\n"
                                    
                                msg += "\n💡 위 종목 매도 후 '매도대금 담보대출'로 미수를 방어하십시오."
                                
                            await send_tg_message(msg)
                            continue

                    if cmd == '중지':
                        is_paused = True
                        await send_tg_message("🛑 [시스템 수동 중지] 모든 감시와 API 통신을 차단합니다.")
                        continue
                    if cmd == '가동':
                        is_paused = False
                        await send_tg_message("▶️ [시스템 수동 가동] 감시 및 API 통신을 정상 재개합니다.")
                        continue
                    if cmd == '분석초기화':
                        if os.path.exists('gemini_log.csv'): os.rename('gemini_log.csv', f'gemini_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                        if os.path.exists('laptop_log.csv'): os.rename('laptop_log.csv', f'laptop_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                        await send_tg_message(f"♻️ 딥러닝 누적 데이터가 백업/초기화되었습니다.")
                        continue
                        
                    if cmd == '분석':
                        msg = "📊 [퀀트 누적 통계 분석 리포트]\n"
                        def _build_report(file_path, engine_name, base_amt):
                            if not os.path.exists(file_path): return ""
                            try:
                                df = pd.read_csv(file_path)
                                if len(df) == 0: return f"\n[{engine_name}] 기록된 데이터가 없습니다.\n"
                                def _categorize(row):
                                    reason = str(row.get('ExitReason', ''))
                                    pnl = float(row.get('PnL(%)', 0))
                                    if '익절' in reason or '목표가' in reason: return 'Win'
                                    elif '본절' in reason: return 'Draw'
                                    else: return 'Loss'
                                df['Category'] = df.apply(_categorize, axis=1)
                                total_cnt = len(df)
                                win_cnt = len(df[df['Category'] == 'Win'])
                                draw_cnt = len(df[df['Category'] == 'Draw'])
                                loss_cnt = len(df[df['Category'] == 'Loss'])
                                win_rate = (win_cnt / total_cnt) * 100 if total_cnt > 0 else 0
                                
                                df['EstPnL'] = df['PnL(%)'] / 100.0 * base_amt
                                total_pnl = df['EstPnL'].sum()
                                avg_win = df[df['Category'] == 'Win']['PnL(%)'].mean() if win_cnt > 0 else 0
                                avg_loss = df[df['Category'] == 'Loss']['PnL(%)'].mean() if loss_cnt > 0 else 0
                                
                                rep = f"\n🔥 [{engine_name} 엔진]\n"
                                rep += f"• 거래량: {total_cnt}건 (승 {win_cnt} / 무 {draw_cnt} / 패 {loss_cnt})\n"
                                rep += f"• 승률: {win_rate:.1f}%\n"
                                rep += f"• 추정 실현손익: {int(total_pnl):,}원\n"
                                rep += f"• 평균 익절: +{avg_win:.2f}% / 평균 손절: {avg_loss:.2f}%\n"
                                return rep
                            except Exception as e:
                                return f"\n❌ {engine_name} 분석 오류: {e}\n"

                        msg += _build_report('gemini_log.csv', '제미나이 스캘핑', user_settings.get('gemini_amount', 500000))
                        msg += _build_report('laptop_log.csv', '랩탑 스윙', user_settings.get('gemini_amount', 500000))
                        await send_tg_message(msg)
                        continue

                    if cmd == '진단':
                        top_20 = await kiwoom_api.get_top_20_search_rank(session)
                        if top_20:
                            c = list(top_20.keys())[0]
                            name = top_20[c]
                            msg = f"🩺 [엔진 실시간 정밀 진단]\n• 타겟: {name} ({c})\n\n"
                            c3 = await kiwoom_api.get_candles(session, c, '3')
                            c5 = await kiwoom_api.get_candles(session, c, '5')
                            
                            if not c3 or not c5:
                                await send_tg_message("❌ 분봉 데이터 수신 오류 (엔진 정지)")
                                continue
                                
                            is_gem, gem_data = strategy.check_gemini_momentum_model(c3, today_str, tp_pct=user_settings.get('gemini_tp_pct', 1.5), sl_pct=user_settings.get('gemini_sl_pct', 1.0), filter_lvl=user_settings.get('gemini_filter_lvl', 3))
                            msg += f"🤖 제미나이(3m): {'✅ 타점 포착 ('+str(gem_data.get('entry_price'))+'원)' if is_gem else '❌ 조건 불충족'}\n"
                            
                            is_lap, lap_data = strategy.check_laptop_swing_model(c5, today_str, user_settings['risk_amount'])
                            msg += f"💻 랩탑 스윙(5m): {'✅ 자동 타점 포착 ('+str(lap_data.get('entry_price'))+'원)' if is_lap else '❌ 조건 불충족'}\n"
                            await send_tg_message(msg)
                        continue

                    if awaiting_setting:
                        if awaiting_setting == 'watch_add':
                            code = cmd.strip()
                            if len(code) == 6 and code.isdigit():
                                watchlist = user_settings.setdefault('custom_watchlist', {})
                                if code in watchlist:
                                    await send_tg_message("❌ 이미 등록된 종목입니다.")
                                elif len(watchlist) >= 10:
                                    await send_tg_message("❌ 관심종목은 최대 10개까지만 등록 가능합니다.")
                                else:
                                    await send_tg_message(f"🔍 [{code}] 네이버 금융 API 역산 중...")
                                    name, mkt = await get_stock_info_from_naver(session, code)
                                    if name:
                                        watchlist[code] = name
                                        _STOCK_MARKET_CACHE[code] = mkt
                                        def _save_set():
                                            with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                        await asyncio.to_thread(_save_set)
                                        await send_tg_message(f"✅ 관심종목 [{name} ({code})] 추가 완료.")
                                    else:
                                        await send_tg_message("❌ 종목명을 찾을 수 없습니다.")
                            else: await send_tg_message("❌ 올바른 6자리 종목코드를 입력하세요.")
                            awaiting_setting = None
                            continue
                        elif awaiting_setting == 'target_date':
                            date_str = cmd.strip()
                            try:
                                datetime.strptime(date_str, '%Y-%m-%d')
                                user_settings['target_date'] = date_str
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                await send_tg_message(f"✅ 목표일이 {date_str}로 변경되었습니다.")
                            except ValueError:
                                await send_tg_message("❌ 날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식으로 입력하세요.")
                            finally:
                                awaiting_setting = None
                            continue
                        else:
                            try:
                                val = float(cmd.replace(',', '').strip())
                                if awaiting_setting in ['base', 'risk']: user_settings[f"{awaiting_setting}_amount"] = int(val)
                                elif awaiting_setting == 'gemini_amount': user_settings['gemini_amount'] = int(val)
                                elif awaiting_setting == 'gemini_tp': user_settings['gemini_tp_pct'] = val
                                elif awaiting_setting == 'gemini_sl': user_settings['gemini_sl_pct'] = val
                                elif awaiting_setting == 'autorr': user_settings['auto_rr_ratio'] = val
                                elif awaiting_setting == 'target_amount': user_settings['target_amount'] = int(val)
                                    
                                await send_tg_message("✅ 설정 변경이 완료되었습니다.")
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                            except ValueError: await send_tg_message("❌ 숫자만 입력해야 합니다.")
                            finally: awaiting_setting = None
                            continue

                    if cmd == '세팅':
                        btn_gem = "🟢 제미나이 ON" if user_settings.get('engine_gem_on', True) else "🔴 제미나이 OFF"
                        btn_lap = "🟢 랩탑 스윙 ON" if user_settings.get('engine_laptop_on', True) else "🔴 랩탑 스윙 OFF"
                        btn_time = "⏱️ 시간대 필터 ON" if user_settings.get('time_filter_on', False) else "⏱️ 시간대 필터 OFF"
                        btn_sync = "🟢 미보유 자동삭제 ON" if user_settings.get('auto_remove_unheld', False) else "🔴 미보유 자동삭제 OFF"
                        nxt_status = "🟢 ON (연장장 포함)" if user_settings.get('nxt_scan_enabled', False) else "🔴 OFF (정규장 전용)"
                        btn_gem_lvl = f"🎚️ 제미나이 필터: Lv.{user_settings.get('gemini_filter_lvl', 3)}"
                        
                        msg = f"⚙️ [시스템 세팅 현황]\n\n"
                        msg += f"📘 포트폴리오 및 랩탑 스윙 설정\n"
                        msg += f"• 1회 허용 손실(Risk): {user_settings.get('risk_amount', 500000):,}원\n"
                        msg += f"• 스윙 손익비(R:R): 1 : {user_settings.get('auto_rr_ratio', 2.0)}\n\n"
                        
                        msg += f"🔥 제미나이 스캘핑 전용 설정\n"
                        msg += f"• 현재 필터 강도: Lv.{user_settings.get('gemini_filter_lvl', 3)}\n"
                        msg += f"• 고정 진입 금액: {user_settings.get('gemini_amount', 500000):,}원\n"
                        msg += f"• 자동 익절(TP): +{user_settings.get('gemini_tp_pct', 1.5)}%\n"
                        msg += f"• 자동 손절(SL): -{user_settings.get('gemini_sl_pct', 1.0)}%\n\n"
                        
                        msg += f"🌌 NXT 스캔 모드: {nxt_status}\n\n"
                        msg += f"🎛️ 엔진 스위치 (클릭하여 변경)\n"
                        
                        reply_markup = {
                            "inline_keyboard": [
                                [{"text": btn_gem, "callback_data": "toggle_gem"}, {"text": btn_lap, "callback_data": "toggle_lap"}],
                                [{"text": btn_time, "callback_data": "toggle_time"}, {"text": btn_sync, "callback_data": "toggle_sync"}],
                                [{"text": btn_gem_lvl, "callback_data": "cycle_gem_lvl"}, {"text": "자동 진입금액", "callback_data": "set_gemini_amount"}],
                                [{"text": "수동 손실비용", "callback_data": "set_risk"}, {"text": "스윙 손익비", "callback_data": "set_autorr"}],
                                [{"text": "NXT 스캔 전환", "callback_data": "toggle_nxt"}, {"text": "기준 자산 갱신", "callback_data": "set_base"}]
                            ]
                        }
                        await send_tg_message(msg, reply_markup=reply_markup)
                        continue

                    if cmd.startswith('cb:'):
                        cb_data = cmd.replace('cb:', '')
                        
                        if cb_data == 'cycle_gem_lvl':
                            lvl = user_settings.get('gemini_filter_lvl', 3)
                            lvl = lvl + 1 if lvl < 3 else 1
                            user_settings['gemini_filter_lvl'] = lvl
                            def _save_set():
                                with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                            await asyncio.to_thread(_save_set)
                            
                            msg = f"✅ 제미나이 필터가 **Lv.{lvl}** 로 변경되었습니다.\n"
                            if lvl == 1: msg += "• Lv.1 (공격): 거래량 2배 폭발 + 양봉 (가장 많은 타점, 휩쏘 주의)"
                            elif lvl == 2: msg += "• Lv.2 (표준): Lv.1 + 당일 VWAP 지지 확인"
                            elif lvl == 3: msg += "• Lv.3 (보수): Lv.2 + 60선 장기 추세 상회 (가장 엄격함)"
                            await send_tg_message(msg)
                            continue

                        toggle_map = {'toggle_gem': 'engine_gem_on', 'toggle_lap': 'engine_laptop_on', 'toggle_time': 'time_filter_on', 'toggle_sync': 'auto_remove_unheld'}
                        if cb_data in toggle_map:
                            key = toggle_map[cb_data]
                            user_settings[key] = not user_settings.get(key, True if 'engine' in key else False)
                            def _save_set():
                                with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                            await asyncio.to_thread(_save_set)
                            status = "🟢 활성화" if user_settings[key] else "🔴 비활성화"
                            await send_tg_message(f"✅ 설정이 **{status}** 되었습니다.")
                            continue

                        if cb_data == 'toggle_nxt':
                            user_settings['nxt_scan_enabled'] = not user_settings.get('nxt_scan_enabled', False)
                            def _save_set():
                                with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                            await asyncio.to_thread(_save_set)
                            new_status = "ON" if user_settings['nxt_scan_enabled'] else "OFF"
                            await send_tg_message(f"✅ NXT 연장장 스캔 모드가 **{new_status}** 되었습니다.")
                            continue
                        
                        if cb_data in ['set_base', 'set_risk', 'set_autorr', 'set_gemini_amount', 'set_gemini_tp', 'set_gemini_sl', 'set_target_date', 'set_target_amount']:
                            awaiting_setting = cb_data.replace('set_', '')
                            if awaiting_setting == 'target_date':
                                await send_tg_message("🗓️ 새로운 목표일을 YYYY-MM-DD 형식으로 입력하세요 (예: 2026-12-20):")
                            else:
                                await send_tg_message("✏️ 새로운 값을 숫자로만 입력하세요.")
                            continue

                    if cmd == '잔고':
                        bal = await kiwoom_api.get_account_balance(session)
                        if bal: await send_tg_message(bal.replace('📊 ', '').replace('📋 ', ''))
                        continue

                    if cmd == '감시취소':
                        cancel_count = 0
                        for code, cond in list(auto_watch_list.items()):
                            if cond.get('status') == 'pending' and cond.get('odno'):
                                await kiwoom_api.cancel_order(session, code, cond['odno'])
                                cancel_count += 1
                            if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                        auto_watch_list.clear()
                        await save_watch_list(redis_client, auto_watch_list, use_redis)
                        await send_tg_message(f"🛑 전체 관제 취소 완료. (미체결 동기화 취소: {cancel_count}건)")
                        continue

                    if cmd == '감시':
                        if not auto_watch_list: 
                            await send_tg_message("현재 자동감시 없음.")
                        else:
                            watch_msg = "👀 [현재 감시 현황]\n\n"
                            for code, cond in auto_watch_list.items():
                                status_str = "🟢 활성" if cond['status'] == 'active' else "⏳ 대기"
                                engine_str = "제미나이 스캘핑" if cond.get('is_gemini') else "랩탑 스윙"
                                qty_str = f"({cond.get('qty', 0)}주)"
                                watch_msg += f"🔹 {cond.get('name', code)} {qty_str} - {engine_str} / {status_str}\n"
                            
                            reply_markup = {"inline_keyboard": [[{"text": "♻️ 잔고 강제 동기화 (수동매도 정리)", "callback_data": "sync_holdings"}]]}
                            await send_tg_message(watch_msg, reply_markup=reply_markup)
                        continue

                await asyncio.sleep(0.5)

        # 2. 주기적 스케줄링 태스크
        async def task_scheduler():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date
            
            while True:
                current_timestamp = time.time()
                now = datetime.now(KST) 
                today_str = now.strftime('%Y%m%d')
                is_weekend = (now.weekday() >= 5)
                is_active_day = not is_weekend and not is_paused

                # 매일 아침 초기화
                if now.hour == 7 and now.minute == 55 and last_daily_reset_date != today_str:
                    asset_history.clear()    
                    max_assets_today = 0
                    max_assets_time = ""
                    is_paused = False 
                    alerted_obs.clear()
                    pending_setups.clear()
                    await save_alerted_obs(alerted_obs)
                    last_daily_reset_date = today_str
                    last_engine_scan_time = "스캔 대기 중"
                    await send_tg_message("🌅 [07:55] 시스템 메모리 초기화. 금일 데이터를 준비합니다.")
                
                # 매 정각 화면 지우기
                if now.minute == 0 and last_cleared_hour != now.hour:
                    os.system('cls' if os.name == 'nt' else 'clear')
                    last_cleared_hour = now.hour

                # 1분 단위 잔고 상태 동기화 (수동 매도 감지)
                if is_active_day and (current_timestamp - last_sync_time > 60):
                    if auto_watch_list and user_settings.get('auto_remove_unheld', False):
                        holdings = await kiwoom_api.get_holdings_data(session)
                        if holdings is not None:
                            state_changed = False
                            for code, cond in list(auto_watch_list.items()):
                                if current_timestamp - cond.get('entry_time', current_timestamp) > 60:
                                    if code not in holdings or holdings[code].get('qty', 0) == 0:
                                        await send_tg_message(f"♻️ [상태 동기화] {cond.get('name', code)} 미보유 감지. 감시 목록에서 자동 제거합니다.")
                                        del auto_watch_list[code]
                                        if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                        state_changed = True
                            if state_changed: await save_watch_list(redis_client, auto_watch_list, use_redis)
                    last_sync_time = current_timestamp

                # 15:20 미수방어용 스냅샷 기록
                if is_active_day and now.hour == 15 and now.minute == 20 and last_snapshot_date != today_str:
                    holdings_snap = await kiwoom_api.get_holdings_data(session)
                    if holdings_snap is not None:
                        snap_data = {}
                        if os.path.exists(SNAPSHOT_FILE):
                            try:
                                with open(SNAPSHOT_FILE, 'r') as f:
                                    snap_data = json.load(f)
                            except: pass
                        snap_data[today_str] = holdings_snap
                        keys_to_keep = sorted(snap_data.keys())[-10:]
                        snap_data = {k: snap_data[k] for k in keys_to_keep}
                        def _save_snap():
                            with open(SNAPSHOT_FILE, 'w') as f:
                                json.dump(snap_data, f, indent=4)
                        await asyncio.to_thread(_save_snap)
                        last_snapshot_date = today_str
                        await send_tg_message("📸 [15:20] 미수정리용 D-2 잔고 스냅샷이 안전하게 기록되었습니다.")

                # 실시간 자산 기록 (50초 이상 주기)
                is_notify_time = (dt_time(8, 0) <= now.time() <= dt_time(20, 0)) and is_active_day
                if is_notify_time and now.second >= 3 and (current_timestamp - last_asset_record_time > 50):
                    is_premarket = (now.hour == 8 and 50 <= now.minute <= 59)
                    is_open_lag = (now.hour == 9 and now.minute == 0)
                    if not is_premarket and not is_open_lag:
                        current_assets = await kiwoom_api.get_estimated_assets(session)
                        if current_assets is not None:
                            if current_assets > max_assets_today:
                                max_assets_today, max_assets_time = current_assets, now.strftime('%H:%M:%S')
                            asset_history.append((now, current_assets))
                            await asyncio.to_thread(lambda: csv.writer(open(ASSET_FILE, 'a', newline='')).writerow([now.strftime('%Y-%m-%d %H:%M:%S'), current_assets]))
                    last_asset_record_time = time.time()

                # 자산 차트 10분마다 자동 발송
                if is_notify_time and now.minute % 10 == 0 and now.second >= 10 and (current_timestamp - last_auto_chart_time > 60):
                    if len(asset_history) >= 2:
                        await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                    last_auto_chart_time = current_timestamp

                await asyncio.sleep(1)

        # 3. 시장 조건 검색 스캐너 태스크
        async def task_scanner():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date
            
            while True:
                current_timestamp = time.time()
                now = datetime.now(KST) 
                today_str = now.strftime('%Y%m%d')
                is_weekend = (now.weekday() >= 5)
                is_active_day = not is_weekend and not is_paused

                scan_end_time = dt_time(20, 0) if user_settings.get('nxt_scan_enabled', False) else dt_time(15, 30)
                is_scan_time = (dt_time(9, 0) <= now.time() <= scan_end_time) and is_active_day

                allow_gem = user_settings.get('engine_gem_on', True)
                allow_lap = user_settings.get('engine_laptop_on', True)
                
                if user_settings.get('time_filter_on', False):
                    if not (9 <= now.hour <= 10 and (now.hour == 9 or now.minute <= 30)):
                        allow_gem, allow_lap = False, False

                # 시장 서킷 브레이커 방어 (과거 등락률 로직 유지)
                if current_macro_pct <= -1.5:
                    allow_gem, allow_lap = False, False

                if is_scan_time and (current_timestamp - last_scan_time > 50):
                    is_3m = (now.minute % 3 == 0)
                    is_5m = (now.minute % 5 == 0)
                    
                    if is_3m or is_5m:
                        current_macro_pct = await get_kosdaq_macro_trend(session) 
                        
                        # 🚨 1. 지수 분봉 수집 및 5MA/20MA 연산 (정배열/역배열 판별)
                        kpi_candles = await get_macro_minute_candles(session, 'KOSPI')
                        kdq_candles = await get_macro_minute_candles(session, 'KOSDAQ')
                        
                        macro_state = {'KOSPI': {'trend': '정배열', 'gap': 0.0, '5ma': 0, '20ma': 0}, 
                                       'KOSDAQ': {'trend': '정배열', 'gap': 0.0, '5ma': 0, '20ma': 0}}
                                       
                        def _calc_ma(candles):
                            if len(candles) < 20: return 0, 0, '정배열', 0.0
                            closes = [c['close'] for c in candles[:20]]
                            ma5 = sum(closes[:5]) / 5
                            ma20 = sum(closes[:20]) / 20
                            trend = '정배열' if ma5 >= ma20 else '역배열'
                            gap = ((ma5 - ma20) / ma20) * 100 if ma20 > 0 else 0.0
                            return ma5, ma20, trend, gap
                            
                        kpi_5, kpi_20, kpi_trend, kpi_gap = _calc_ma(kpi_candles)
                        kdq_5, kdq_20, kdq_trend, kdq_gap = _calc_ma(kdq_candles)
                        
                        macro_state['KOSPI'] = {'trend': kpi_trend, 'gap': round(kpi_gap, 2), '5ma': kpi_5, '20ma': kpi_20}
                        macro_state['KOSDAQ'] = {'trend': kdq_trend, 'gap': round(kdq_gap, 2), '5ma': kdq_5, '20ma': kdq_20}
                        
                        # 타겟 후보 수집
                        search_20 = await kiwoom_api.get_top_20_search_rank(session)
                        volume_20 = await kiwoom_api.get_top_20_volume_rank(session)
                        
                        target_candidates = {}
                        if search_20: target_candidates.update(search_20)
                        if volume_20: target_candidates.update(volume_20)
                        
                        target_codes = list(target_candidates.keys())
                        stock_dict.update(target_candidates)
                        
                        watchlist = user_settings.get('custom_watchlist', {})
                        for wc_code, wc_name in watchlist.items():
                            if wc_code not in target_codes:
                                target_codes.append(wc_code)
                                stock_dict[wc_code] = wc_name
                                
                        # 🚨 2. 포착 종목의 소속 시장 실시간 판별 및 캐싱
                        async def fetch_market_if_needed(c):
                            if c not in _STOCK_MARKET_CACHE:
                                name, mkt = await get_stock_info_from_naver(session, c)
                                if name:
                                    _STOCK_MARKET_CACHE[c] = mkt
                                    stock_dict[c] = name
                                    
                        await asyncio.gather(*[fetch_market_if_needed(c) for c in target_codes])

                        if target_codes:
                            holdings_check = await kiwoom_api.get_holdings_data(session) or {}
                            async def fetch_with_sem(code, tf):
                                async with api_semaphore: 
                                    await asyncio.sleep(0.1) 
                                    return await kiwoom_api.get_candles(session, code, tf)
                                    
                            candles_3m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '3') for code in target_codes]))) if is_3m else {}
                            candles_5m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '5') for code in target_codes]))) if is_5m else {}
                            
                            for code in target_codes:
                                if code in holdings_check: continue
                                
                                market = _STOCK_MARKET_CACHE.get(code, 'KOSDAQ')
                                m_state = macro_state.get(market, {})
                                
                                # 🚨 3. 정배열 킬 스위치 (역배열 시 매수 전면 차단)
                                if m_state.get('trend') == '역배열':
                                    continue
                                
                                curr_price_check = None
                                if is_3m and code in candles_3m_dict and candles_3m_dict[code]:
                                    curr_price_check = abs(int(candles_3m_dict[code][0]['close']))
                                elif is_5m and code in candles_5m_dict and candles_5m_dict[code]:
                                    curr_price_check = abs(int(candles_5m_dict[code][0]['close']))
                                    
                                if curr_price_check is not None and curr_price_check < 1000:
                                    continue
                                        
                                if is_3m and code in candles_3m_dict:
                                    candles_3m = candles_3m_dict[code]
                                    if not candles_3m or not candles_3m[0]['time'].startswith(today_str): continue
                                    
                                    last_engine_scan_time = now.strftime('%H:%M:%S')

                                    if allow_gem:
                                        is_gemini, gemini_data = strategy.check_gemini_momentum_model(
                                            candles_3m, today_str, 
                                            tp_pct=user_settings.get('gemini_tp_pct', 1.5), 
                                            sl_pct=user_settings.get('gemini_sl_pct', 1.0), 
                                            filter_lvl=user_settings.get('gemini_filter_lvl', 3)
                                        )
                                        if is_gemini:
                                            alert_key = f"{code}_3m_GEMINI_{gemini_data['time']}"
                                            if alert_key not in alerted_obs:
                                                alerted_obs.add(alert_key)
                                                await save_alerted_obs(alerted_obs)
                                                entry = gemini_data['entry_price']
                                                qty = user_settings.get('gemini_amount', 500000) // entry
                                                if qty > 0:
                                                    buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, entry)
                                                    if "✅" in buy_res:
                                                        gemini_data['meta']['macro_pct'] = current_macro_pct 
                                                        gemini_data['meta']['macro_trend'] = m_state.get('trend', '정배열')
                                                        gemini_data['meta']['macro_gap'] = m_state.get('gap', 0.0)
                                                        auto_watch_list[code] = {'name': stock_dict[code], 'market': market, 'qty': qty, 'entry': entry, 'sl': gemini_data['sl_price'], 'tp': gemini_data['dynamic_tp'], 'status': 'pending', 'odno': odno, 'is_gemini': True, 'meta': gemini_data['meta'], 'entry_time': time.time()}
                                                        await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                        if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                        msg = f"🤖 [제미나이 눌림목 대기매수] {stock_dict[code]} {qty}주\n"
                                                        msg += f"• 대기 매수가: {entry:,}원 (지정가)\n"
                                                        msg += f"• 체결 시 목표가: {gemini_data['dynamic_tp']:,}원 (+{user_settings.get('gemini_tp_pct', 1.5)}%)\n"
                                                        msg += f"• 체결 시 손절가: {gemini_data['sl_price']:,}원 (-{user_settings.get('gemini_sl_pct', 1.0)}%)\n"
                                                        msg += f"• 진단: {gemini_data['meta'].get('diag_msg', '확인불가')}"
                                                        await send_tg_message(msg)
                                                continue 

                                if is_5m and code in candles_5m_dict:
                                    candles_5m = candles_5m_dict[code]
                                    if not candles_5m or not candles_5m[0]['time'].startswith(today_str): continue

                                    last_engine_scan_time = now.strftime('%H:%M:%S')

                                    if allow_lap:
                                        is_lap, lap_data = strategy.check_laptop_swing_model(candles_5m, today_str, user_settings['risk_amount'], user_settings.get('auto_rr_ratio', 2.0))
                                        if is_lap:
                                            alert_key = f"{code}_5m_LAPTOP_{lap_data['time']}"
                                            if alert_key not in alerted_obs:
                                                alerted_obs.add(alert_key)
                                                await save_alerted_obs(alerted_obs)
                                                entry = lap_data['entry_price']
                                                qty = lap_data['qty'] 
                                                if qty > 0:
                                                    buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, entry)
                                                    if "✅" in buy_res:
                                                        lap_data['meta']['macro_pct'] = current_macro_pct 
                                                        lap_data['meta']['macro_trend'] = m_state.get('trend', '정배열')
                                                        lap_data['meta']['macro_gap'] = m_state.get('gap', 0.0)
                                                        auto_watch_list[code] = {'name': stock_dict[code], 'market': market, 'qty': qty, 'entry': entry, 'sl': lap_data['sl_price'], 'tp': lap_data['dynamic_tp'], 'status': 'pending', 'odno': odno, 'is_laptop': True, 'meta': lap_data['meta'], 'entry_time': time.time()}
                                                        await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                        if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                        await send_tg_message(f"💻 [랩탑 스윙 대기매수] {stock_dict[code]} {qty}주 (매수가: {entry:,}원)")
                                            continue
                    last_scan_time = time.time()
                await asyncio.sleep(1)

        # 4. 실시간 웹소켓 가격 감시 태스크 (블로킹 우회)
        async def task_monitor():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date
            
            while True:
                current_timestamp = time.time()
                now = datetime.now(KST)
                is_weekend = (now.weekday() >= 5)
                is_active_day = not is_weekend and not is_paused

                if is_active_day and auto_watch_list and (current_timestamp - last_monitor_time >= 1):
                    state_changed = False
                    
                    for code, cond in list(auto_watch_list.items()):
                        rt_price = kiwoom_api.realtime_prices.get(code)
                        if not rt_price:
                            async with api_semaphore:
                                c1 = await kiwoom_api.get_candles(session, code, '1')
                            if not c1: continue
                            rt_price = abs(int(c1[0]['close']))
                            
                        stock_name = cond.get('name', code)
                        
                        if cond['status'] == 'pending':
                            if rt_price <= cond['entry']:
                                cond['status'] = 'active'
                                cond['max_reached'] = rt_price
                                cond['min_reached'] = rt_price # 🚨 MAE 추적 시작점
                                state_changed = True
                                await send_tg_message(f"🟢 [{stock_name}] 눌림목 매수 체결 확인! 감시 활성화.")
                            continue 
                        
                        if cond['status'] == 'active':
                            if 'max_reached' not in cond: cond['max_reached'] = rt_price
                            if 'min_reached' not in cond: cond['min_reached'] = rt_price
                            
                            # 🚨 최고점과 최저점 동시 갱신
                            if rt_price > cond['max_reached']: cond['max_reached'] = rt_price
                            if rt_price < cond['min_reached']: cond['min_reached'] = rt_price
                                
                            # 1. 본절 방어 (+1.0%)
                            if rt_price >= cond['entry'] * 1.01 and cond['sl'] < cond['entry']:
                                cond['sl'] = cond['entry']
                                state_changed = True
                                await send_tg_message(f"🛡️ [{stock_name}] +1.0% 수익 도달! 손절가를 매수가({cond['entry']:,}원)로 상향 조정하여 원금을 방어합니다.")

                            # 2. 50% 분할 익절 (+1.5% 목표가 도달 시)
                            if cond['tp'] > 0 and rt_price >= cond['tp']:
                                sell_qty = max(1, cond.get('qty', 1) // 2)
                                sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                                
                                if "✅" in sell_res:
                                    await send_tg_message(f"🚀 [{stock_name}] 1차 목표가({cond['tp']:,}원) 도달! 50% 분할 익절 청산(시장가).\n결과: {sell_res}")
                                    pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                    max_pnl = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                    min_pnl = round(((cond['min_reached'] - cond['entry']) / cond['entry']) * 100, 2) # 🚨 MAE 기록
                                    slippage = round(((rt_price - cond['tp']) / cond['tp']) * 100, 2) if cond['tp'] > 0 else 0.0 # 🚨 체결 오차 기록
                                    session_str = get_time_session(cond.get('entry_time', time.time())) # 🚨 타임 세션 기록
                                    
                                    hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                    meta = cond.get('meta', {})
                                        
                                    # 🚨 확장된 18개 로깅 헤더 구조에 맞게 기록 데이터 갱신
                                    if cond.get('is_gemini'):
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'VolBurstRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, "50%분할익절"]
                                        await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                    elif cond.get('is_laptop'):
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'PullbackVolRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('pullback_vol_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, "50%분할익절"]
                                        await asyncio.to_thread(write_trade_log, 'laptop_log.csv', headers, row)
                                    
                                    cond['qty'] -= sell_qty
                                    if cond['qty'] <= 0:
                                        del auto_watch_list[code]
                                        if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                    else:
                                        cond['tp'] = 0
                                        cond['half_sold'] = True
                                    state_changed = True
                                else:
                                    await send_tg_message(f"⚠️ [{stock_name}] 50% 익절 실패! 수동 확인 요망.\n결과: {sell_res}")
                                    cond['tp'] = 0 
                                    state_changed = True

                            # 3. 트레일링 스탑 (절반 익절 후)
                            if cond.get('half_sold'):
                                new_sl_raw = int(cond['max_reached'] * 0.98)
                                tick = strategy.get_tick_size(new_sl_raw) if hasattr(strategy, 'get_tick_size') else 1
                                new_sl = (new_sl_raw // tick) * tick if tick > 1 else new_sl_raw
                                    
                                if new_sl > cond['sl']:
                                    cond['sl'] = new_sl
                                    state_changed = True

                            # 4. 손절가(또는 트레일링 스탑, 본절가) 이탈 시 전량 청산
                            if code in auto_watch_list and cond['sl'] > 0 and rt_price <= cond['sl']:
                                sell_qty = cond.get('qty', 1)
                                sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                                
                                if "✅" in sell_res:
                                    reason = "트레일링스탑(익절)" if cond.get('half_sold') else ("본절방어" if cond['sl'] == cond['entry'] else "손절(SL)")
                                    icon = "💸" if reason == "트레일링스탑(익절)" else ("🛡️" if reason == "본절방어" else "🔴")
                                    await send_tg_message(f"{icon} [{stock_name}] 손절/익절선({cond['sl']:,}원) 이탈! 남은 수량 전량 청산(시장가).\n사유: {reason}\n결과: {sell_res}")
                                    
                                    pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                    max_pnl = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                    min_pnl = round(((cond['min_reached'] - cond['entry']) / cond['entry']) * 100, 2) # 🚨 MAE 기록
                                    slippage = round(((rt_price - cond['sl']) / cond['sl']) * 100, 2) if cond['sl'] > 0 else 0.0 # 🚨 체결 오차 기록
                                    session_str = get_time_session(cond.get('entry_time', time.time())) # 🚨 타임 세션 기록
                                    
                                    hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                    meta = cond.get('meta', {})
                                        
                                    if cond.get('is_gemini'):
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'VolBurstRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, reason]
                                        await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                    elif cond.get('is_laptop'):
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'PullbackVolRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('pullback_vol_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, reason]
                                        await asyncio.to_thread(write_trade_log, 'laptop_log.csv', headers, row)
                                            
                                    del auto_watch_list[code]
                                    if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                    state_changed = True
                                else:
                                    await send_tg_message(f"❌ [{stock_name}] 최종 전량 청산 매도 실패.\n사유: {sell_res}")
                                    del auto_watch_list[code]
                                    if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                    state_changed = True
                        
                    if state_changed: await save_watch_list(redis_client, auto_watch_list, use_redis)
                    last_monitor_time = time.time()
                
                await asyncio.sleep(0.5)

        # ---------------------------------------------------------------------
        # 멀티 태스킹 동시 실행 시작
        # ---------------------------------------------------------------------
        await asyncio.gather(
            task_telegram(),
            task_scheduler(),
            task_scanner(),
            task_monitor()
        )

if __name__ == '__main__':
    asyncio.run(main())