import os
import time
import csv
import json
import asyncio
import aiohttp
import re
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

chart_lock = threading.Lock()
KST = timezone(timedelta(hours=9))

def get_d_minus_2_date(current_date_str):
    dt = datetime.strptime(current_date_str, '%Y%m%d')
    days_subtracted = 0
    while days_subtracted < 2:
        dt -= timedelta(days=1)
        if dt.weekday() < 5: 
            days_subtracted += 1
    return dt.strftime('%Y%m%d')

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
        'rr_ratio': 1.5,
        'auto_rr_ratio': 2.0, 
        'min_tick_diff': 3,
        'max_entry_gap_ticks': 50, 
        'gemini_amount': 500000,
        'gemini_tp_pct': 1.5,
        'gemini_sl_pct': 1.0,
        'gemini_filter_lvl': 3,
        'nxt_scan_enabled': False,
        'max_holdings': 3,
        'custom_watchlist': {},
        'engine_gem_on': True,
        'engine_laptop_on': True, 
        'engine_ob_on': True,
        'engine_bpr_on': True,
        'time_filter_on': False,
        'circuit_breaker_on': False
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

async def get_stock_name_from_naver(session, code):
    url = f"https://m.stock.naver.com/api/stock/{code}/basic"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'
    }
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            data = await resp.json()
            if data and 'stockName' in data:
                return data['stockName'].strip()
    except Exception as e:
        print(f"네이버 모바일 API 종목명 역산 오류: {e}")
    return None

async def get_kosdaq_macro_trend(session):
    url = "https://m.stock.naver.com/api/index/KOSDAQ/basic"
    headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/80.0.3987.162 Mobile Safari/537.36'}
    try:
        async with session.get(url, headers=headers, timeout=3) as resp:
            data = await resp.json()
            return float(data.get('fluctuationsRatio', 0.0))
    except Exception:
        return 0.0

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
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xticks(rotation=45)
        plt.tight_layout()
        chart_path = 'asset_chart.png'
        plt.savefig(chart_path)
        plt.close()
        
    diff = assets[-1] - base_amount
    diff_str = f"+{diff:,}" if diff > 0 else f"{diff:,}"
    caption = f"📊 현재 자산: {assets[-1]:,}원 ({diff_str}원)"
    if max_assets_today > 0:
        max_diff = max_assets_today - base_amount
        caption += f"\n👑 최고 자산: {max_assets_today:,}원 ({f'+{max_diff:,}' if max_diff>0 else f'{max_diff:,}'}원) - {max_assets_time} 기준"
    telegram_bot.send_photo(chart_path, caption=caption)
    return True

def _generate_and_send_candle_chart(candles, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name):
    if not candles or len(candles) < 10: return False
    target_candles = list(reversed(candles[:20]))
    times = [c['time'][8:12] for c in target_candles] 
    with chart_lock:
        plt.figure(figsize=(10, 5))
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False
        for i, c in enumerate(target_candles):
            color = 'red' if c['close'] >= c['open'] else 'blue'
            plt.vlines(i, c['low'], c['high'], color=color, linewidth=1)
            bh = abs(c['close'] - c['open'])
            if bh == 0: bh = strategy.get_tick_size(c['close'])
            plt.bar(i, bh, bottom=min(c['open'], c['close']), color=color, width=0.6)
        plt.axhline(y=tp_price, color='green', linestyle='-.', linewidth=2, label='목표가 (TP)')
        plt.axhline(y=entry_price, color='magenta', linestyle='-', linewidth=2, label='매수가 (Entry)')
        plt.axhline(y=sl_price, color='black', linestyle='--', linewidth=2, label='손절가 (SL)')
        plt.title(f"[{stock_name}] {strategy_name} 셋업 ({tic_scope}분봉)")
        plt.xticks(range(0, len(times), 2), times[::2], rotation=45)
        plt.legend(loc='best')
        plt.grid(True, linestyle=':', alpha=0.5)
        plt.tight_layout()
        chart_path = f'candle_{stock_code}.png'
        plt.savefig(chart_path)
        plt.close()
    telegram_bot.send_photo(chart_path, caption=f"👁️‍🗨️ {stock_name} {tic_scope}분봉 [{strategy_name}]")
    try: os.remove(chart_path) 
    except: pass

async def send_candle_chart(session, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name):
    candles = await kiwoom_api.get_candles(session, stock_code, tic_scope)
    await asyncio.to_thread(_generate_and_send_candle_chart, candles, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name)

def write_trade_log(filename, headers, row_data):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        writer.writerow(row_data)

async def main():
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auto_watch_list, alerted_obs, use_redis = await load_persistent_state(redis_client)
    api_semaphore = asyncio.Semaphore(4)
    
    # 🚨 신규 추가: 인메모리 엔진 및 데이터 공백 방지용 관제 세트
    candle_engine = strategy.CandleManager()
    watched_stocks = set()

    stock_dict = {}
    pending_setups = {} 
    
    last_monitor_time = 0
    last_scan_time = 0
    last_asset_record_time = 0
    last_auto_chart_time = 0
    
    last_cleared_hour = None
    last_daily_reset_date = None
    last_snapshot_date = None 
    awaiting_setting = None
    
    is_paused = False
    current_macro_pct = 0.0 
    daily_realized_pnl = 0  
    
    last_engine_scan_time = "스캔 대기 중"
    
    asset_history, max_assets_today, max_assets_time = await asyncio.to_thread(load_today_asset_data)

    async with aiohttp.ClientSession() as session:
        user_settings = await load_or_init_settings(session)
        print(f"\n✅ 시스템 초기화 완료. 백업 관제 {len(auto_watch_list)}개, 알림 로그 {len(alerted_obs)}개 복구.")
        
        kiwoom_api.ws_client = kiwoom_api.KISWebSocket(session)
        await kiwoom_api.ws_client.connect()
        for code in auto_watch_list.keys():
            await kiwoom_api.ws_client.subscribe(code)
            watched_stocks.add(code)

        manual_msg = f"""
🤖 [관제탑 4대 퀀트 전술 스캐너 가동]
💡 **채팅창에 'ㅁ' 을 입력하시면 통합 관제 대시보드가 열립니다.**
        """
        await send_tg_message(manual_msg.strip())

        while True:
            current_timestamp = time.time()
            now = datetime.now(KST) 
            today_str = now.strftime('%Y%m%d')
            
            if now.hour == 7 and now.minute == 55 and last_daily_reset_date != today_str:
                asset_history.clear()    
                max_assets_today = 0
                max_assets_time = ""
                daily_realized_pnl = 0 
                is_paused = False 
                alerted_obs.clear()
                pending_setups.clear()
                await save_alerted_obs(alerted_obs)
                last_daily_reset_date = today_str
                last_engine_scan_time = "스캔 대기 중"
                await send_tg_message("🌅 [07:55] 시스템 메모리 초기화. 금일 데이터를 준비합니다.")
            
            if now.minute == 0 and last_cleared_hour != now.hour:
                os.system('cls' if os.name == 'nt' else 'clear')
                last_cleared_hour = now.hour

            new_commands = await asyncio.to_thread(telegram_bot.fetch_commands)
            
            for cmd in new_commands:
                if cmd == 'ㄱ':
                    if len(asset_history) >= 2:
                        await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                    else:
                        await send_tg_message("⚠️ 아직 자산 차트를 그릴 충분한 데이터가 수집되지 않았습니다 (최소 20분 소요).")
                    continue

                if cmd.startswith('cb:'):
                    cb_data = cmd.replace('cb:', '')
                    if cb_data == 'menu_setting': cmd = '세팅'
                    elif cb_data == 'menu_monitor': cmd = '감시'
                    elif cb_data == 'menu_balance': cmd = '잔고'
                    elif cb_data == 'menu_analysis': cmd = '분석'
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

                if cmd == 'ㅁ':
                    msg = f"🎛️ [메인 관제탑 대시보드]\n"
                    msg += f"📡 최근 엔진 스캔: {last_engine_scan_time}\n\n"
                    msg += "원하시는 메뉴를 터치하십시오."
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "📈 관심종목 관리", "callback_data": "menu_watch"}, {"text": "👀 감시 현황", "callback_data": "menu_monitor"}],
                            [{"text": "💰 계좌 잔고", "callback_data": "menu_balance"}, {"text": "📊 누적 통계 분석", "callback_data": "menu_analysis"}],
                            [{"text": "⚙️ 엔진 세팅", "callback_data": "menu_setting"}],
                            [{"text": "💸 미수/반대매매 방어 (D-2)", "callback_data": "menu_margin_clear"}] 
                        ]
                    }
                    await send_tg_message(msg, reply_markup=reply_markup)
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
                    if os.path.exists('ob_log.csv'): os.rename('ob_log.csv', f'ob_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
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
                                if '익절' in reason or (reason == '무한트레일링' and pnl > 0): return 'Win'
                                elif '본절' in reason or (reason == '무한트레일링' and pnl == 0): return 'Draw'
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

                    msg += _build_report('gemini_log.csv', '제미나 스캘핑', user_settings.get('gemini_amount', 500000))
                    msg += _build_report('laptop_log.csv', '랩탑 스윙', user_settings.get('gemini_amount', 500000))
                    msg += _build_report('ob_log.csv', '오더블럭 스윙', user_settings.get('gemini_amount', 500000)) 
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

                        is_ob3, ob3_data = strategy.check_orderblock_engulfing(c3, today_str, user_settings.get('rr_ratio', 1.5))
                        msg += f"🎯 OB 스윙(3m): {'✅ 수동 타점 포착 ('+str(ob3_data.get('entry_price'))+'원)' if is_ob3 else '❌ 조건 불충족'}\n"
                        
                        is_bpr, bpr_data = strategy.check_bpr_ifvg_model(c5, today_str, user_settings.get('rr_ratio', 1.5))
                        msg += f"🔥 BPR 스윙(5m): {'✅ 수동 타점 포착 ('+str(bpr_data.get('entry_price'))+'원)' if is_bpr else '❌ 조건 불충족'}\n"
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
                                name = await get_stock_name_from_naver(session, code)
                                if name:
                                    watchlist[code] = name
                                    def _save_set():
                                        with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                    await asyncio.to_thread(_save_set)
                                    await send_tg_message(f"✅ 관심종목 [{name} ({code})] 추가 완료.")
                                else:
                                    await send_tg_message("❌ 종목명을 찾을 수 없습니다.")
                        else: await send_tg_message("❌ 올바른 6자리 종목코드를 입력하세요.")
                        awaiting_setting = None
                        continue
                        
                    elif awaiting_setting.startswith('customtp_'):
                        parts = awaiting_setting.split('_')
                        stk_code, qty, entry, sl = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
                        try:
                            new_tp = strategy.floor_to_tick(int(cmd.replace(',', '').strip()))
                            if new_tp <= entry: await send_tg_message("❌ 목표가는 진입가보다 높아야 합니다.")
                            else:
                                stk_name = stock_dict.get(stk_code, stk_code)
                                await send_candle_chart(session, stk_name, stk_code, '5', entry, sl, new_tp, "수동 목표가")
                                msg = f"🎯 [{stk_name} 수동 튜닝]\n• 대기: {entry:,}원\n• 손절: {sl:,}원\n• 익절: {new_tp:,}원\n• 수량: {qty:,}주"
                                reply_markup = {"inline_keyboard": [[{"text": f"🚀 {qty}주 즉시 진입", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{new_tp}"}]]}
                                await send_tg_message(msg, reply_markup=reply_markup)
                        except ValueError: await send_tg_message("❌ 올바른 숫자가 아닙니다.")
                        finally: awaiting_setting = None
                        continue
                    
                    elif awaiting_setting.startswith('manualqty_'):
                        parts = awaiting_setting.split('_')
                        stk_code, entry, sl, tp = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
                        try:
                            qty = int(cmd.replace(',', '').strip())
                            if qty <= 0: await send_tg_message("❌ 수량은 1주 이상이어야 합니다.")
                            else:
                                stk_name = stock_dict.get(stk_code, stk_code)
                                msg = f"🎯 [{stk_name} 부분 수량 진입]\n• 보정 수량: {qty:,}주\n강제 진입을 시도합니다."
                                reply_markup = {"inline_keyboard": [[{"text": f"🚀 {qty}주 강제 진입", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{tp}"}]]}
                                await send_tg_message(msg, reply_markup=reply_markup)
                        except ValueError: await send_tg_message("❌ 취소합니다.")
                        finally: awaiting_setting = None
                        continue
                        
                    else:
                        try:
                            val = float(cmd.replace(',', '').strip())
                            if awaiting_setting in ['base', 'risk']: user_settings[f"{awaiting_setting}_amount"] = int(val)
                            elif awaiting_setting == 'gemini_amount': user_settings['gemini_amount'] = int(val)
                            elif awaiting_setting == 'gemini_tp': user_settings['gemini_tp_pct'] = val
                            elif awaiting_setting == 'gemini_sl': user_settings['gemini_sl_pct'] = val
                            elif awaiting_setting == 'rr': user_settings['rr_ratio'] = val
                            elif awaiting_setting == 'autorr': user_settings['auto_rr_ratio'] = val
                            elif awaiting_setting == 'mintick': user_settings['min_tick_diff'] = int(val)
                            elif awaiting_setting == 'maxgap': user_settings['max_entry_gap_ticks'] = int(val)
                            elif awaiting_setting == 'maxholdings': user_settings['max_holdings'] = int(val)
                                
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
                    btn_ob = "🟢 OB 스윙 ON" if user_settings.get('engine_ob_on', True) else "🔴 OB 스윙 OFF"
                    btn_bpr = "🟢 BPR 스윙 ON" if user_settings.get('engine_bpr_on', True) else "🔴 BPR 스윙 OFF"
                    btn_time = "⏱️ 시간대 필터 ON" if user_settings.get('time_filter_on', False) else "⏱️ 시간대 필터 OFF"
                    btn_cb = "🛡️ 서킷브레이커 ON" if user_settings.get('circuit_breaker_on', False) else "🛡️ 서킷브레이커 OFF"
                    nxt_status = "🟢 ON (연장장 포함)" if user_settings.get('nxt_scan_enabled', False) else "🔴 OFF (정규장 전용)"
                    btn_gem_lvl = f"🎚️ 제미나이 필터: Lv.{user_settings.get('gemini_filter_lvl', 3)}"
                    
                    msg = f"⚙️ [시스템 세팅 현황]\n\n"
                    msg += f"📘 포트폴리오 및 스윙 설정\n"
                    msg += f"• 최대 보유 한도: {user_settings.get('max_holdings', 3)}종목\n"
                    msg += f"• 1회 허용 손실(Risk): {user_settings.get('risk_amount', 500000):,}원\n"
                    msg += f"• 자동 스윙 손익비(R:R): 1 : {user_settings.get('auto_rr_ratio', 2.0)}\n"
                    msg += f"• 수동 스윙 손익비(R:R): 1 : {user_settings.get('rr_ratio', 1.5)}\n"
                    msg += f"• 진입 이격 허용: {user_settings.get('max_entry_gap_ticks', 50)}틱\n"
                    msg += f"• 휩쏘 방어 필터: {user_settings.get('min_tick_diff', 3)}틱\n\n"
                    
                    msg += f"🔥 제미나이 스캘핑 전용 설정\n"
                    msg += f"• 현재 필터 강도: Lv.{user_settings.get('gemini_filter_lvl', 3)}\n"
                    msg += f"• 고정 진입 금액: {user_settings.get('gemini_amount', 500000):,}원\n"
                    msg += f"• 자동 익절(TP): +{user_settings.get('gemini_tp_pct', 1.5)}%\n"
                    msg += f"• 자동 손절(SL): -{user_settings.get('gemini_sl_pct', 1.0)}%\n\n"
                    
                    msg += f"🌌 NXT 스캔 모드: {nxt_status}\n\n"
                    msg += f"🎛️ 4대 엔진 및 보호막 스위치 (클릭하여 변경)\n"
                    
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": btn_gem, "callback_data": "toggle_gem"}, {"text": btn_lap, "callback_data": "toggle_lap"}],
                            [{"text": btn_ob, "callback_data": "toggle_ob"}, {"text": btn_bpr, "callback_data": "toggle_bpr"}],
                            [{"text": btn_time, "callback_data": "toggle_time"}, {"text": btn_cb, "callback_data": "toggle_cb"}],
                            [{"text": btn_gem_lvl, "callback_data": "cycle_gem_lvl"}, {"text": "포트폴리오 한도", "callback_data": "set_maxholdings"}],
                            [{"text": "자동 진입금액", "callback_data": "set_gemini_amount"}, {"text": "자동 손익비", "callback_data": "set_autorr"}],
                            [{"text": "수동 손실비용", "callback_data": "set_risk"}, {"text": "수동 손익비", "callback_data": "set_rr"}],
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

                    toggle_map = {
                        'toggle_gem': 'engine_gem_on', 'toggle_lap': 'engine_laptop_on', 
                        'toggle_ob': 'engine_ob_on', 'toggle_bpr': 'engine_bpr_on',
                        'toggle_time': 'time_filter_on', 'toggle_cb': 'circuit_breaker_on'
                    }
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
                    
                    if cb_data in ['set_base', 'set_risk', 'set_rr', 'set_autorr', 'set_mintick', 'set_maxgap', 'set_gemini_amount', 'set_gemini_tp', 'set_gemini_sl', 'set_maxholdings']:
                        awaiting_setting = cb_data.replace('set_', '')
                        await send_tg_message(f"✏️ 새로운 값을 숫자로만 입력하세요.")
                        continue
                        
                    if cb_data.startswith('chgtp_'):
                        parts = cb_data.split('_')
                        awaiting_setting = f"customtp_{parts[1]}_{parts[2]}_{parts[3]}_{parts[4]}"
                        stk_name = stock_dict.get(parts[1], parts[1])
                        await send_tg_message(f"✏️ [{stk_name}] 새로운 익절가를 입력하세요:")
                        continue

                    if cb_data.startswith('chgqty_'):
                        parts = cb_data.split('_')
                        awaiting_setting = f"manualqty_{parts[1]}_{parts[2]}_{parts[3]}_{parts[4]}"
                        stk_name = stock_dict.get(parts[1], parts[1])
                        await send_tg_message(f"✏️ [{stk_name}] 현재 살 수 있는 최대 수량(주)을 숫자로 입력하세요:")
                        continue
                    
                    if cb_data.startswith('adj_'):
                        parts = cb_data.split('_')
                        adj_type, stk_code = parts[1], parts[2]
                        if adj_type == 'qty':
                            delta, qty, entry, sl, tp, strat = int(parts[3]), int(parts[4]), int(parts[5]), int(parts[6]), int(parts[7]), parts[8]
                            new_qty = max(1, qty + delta)
                            new_tp = tp
                        elif adj_type == 'tp':
                            delta_pct, float_tp = float(parts[3]), float(parts[7])
                            new_tp = int(float_tp * (1 + delta_pct/100.0))
                            try: new_tp = strategy.floor_to_tick(new_tp)
                            except: pass
                            new_qty = int(parts[4])
                            entry, sl, strat = int(parts[5]), int(parts[6]), parts[8]
                            
                        stk_name = stock_dict.get(stk_code, stk_code)
                        rt_price = kiwoom_api.realtime_prices.get(stk_code)
                        if not rt_price:
                            rt_c = await kiwoom_api.get_candles(session, stk_code, '1')
                            rt_price = abs(int(rt_c[0]['close'])) if rt_c else entry
                            
                        diff = strategy.count_exact_ticks(rt_price, entry)
                        sign_str = "+" if entry > rt_price else "-"
                        
                        msg = f"🎯 [{stk_name} 수동 진입 (값 보정됨)]\n• 엔진: {strat}\n• 현재가: {rt_price:,}원\n• 대기: {entry:,}원 ({sign_str}{diff}틱)\n• 손절: {sl:,}원\n• 목표: {new_tp:,}원\n• 수량: {new_qty:,}주"
                        reply_markup = {"inline_keyboard": [
                            [{"text": f"🚀 {new_qty}주 승인", "callback_data": f"buy_{stk_code}_{new_qty}_{entry}_{sl}_{new_tp}"}],
                            [{"text": "➖ 10주", "callback_data": f"adj_qty_{stk_code}_{-10}_{new_qty}_{entry}_{sl}_{new_tp}_{strat}"},
                             {"text": "➕ 10주", "callback_data": f"adj_qty_{stk_code}_{10}_{new_qty}_{entry}_{sl}_{new_tp}_{strat}"}],
                            [{"text": "🔻 TP -0.5%", "callback_data": f"adj_tp_{stk_code}_{-0.5}_{new_qty}_{entry}_{sl}_{new_tp}_{strat}"},
                             {"text": "🔺 TP +0.5%", "callback_data": f"adj_tp_{stk_code}_{0.5}_{new_qty}_{entry}_{sl}_{new_tp}_{strat}"}]
                        ]}
                        await send_tg_message(msg, reply_markup=reply_markup)
                        continue
                    
                    if cb_data.startswith('obcalc_'):
                        parts = cb_data.split('_')
                        if len(parts) < 6: continue
                        stk_code, tic, entry, sl, tp = parts[1], parts[2], int(parts[3]), int(parts[4]), int(parts[5])
                        strategy_name = parts[6] if len(parts) >= 7 else "수동 산출"
                        
                        risk_share = entry - sl
                        qty = user_settings['risk_amount'] // risk_share if risk_share > 0 else 0
                        if qty == 0:
                            await send_tg_message("❌ 매수 불가: 손실비용 초과.")
                            continue
                            
                        stk_name = stock_dict.get(stk_code, stk_code)
                        await send_candle_chart(session, stk_name, stk_code, tic, entry, sl, tp, strategy_name)
                        
                        rt_price = kiwoom_api.realtime_prices.get(stk_code)
                        if not rt_price:
                            rt_c = await kiwoom_api.get_candles(session, stk_code, '1')
                            rt_price = abs(int(rt_c[0]['close'])) if rt_c else entry
                            
                        diff = strategy.count_exact_ticks(rt_price, entry)
                        sign_str = "+" if entry > rt_price else "-"
                        
                        msg = f"🎯 [{stk_name} 수동 진입 대기]\n• 엔진: {strategy_name}\n• 현재가: {rt_price:,}원\n• 대기: {entry:,}원 ({sign_str}{diff}틱)\n• 손절: {sl:,}원\n• 목표: {tp:,}원\n• 수량: {qty:,}주"
                        reply_markup = {"inline_keyboard": [
                            [{"text": f"🚀 {qty}주 승인", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{tp}"}],
                            [{"text": "➖ 10주", "callback_data": f"adj_qty_{stk_code}_{-10}_{qty}_{entry}_{sl}_{tp}_{strategy_name}"},
                             {"text": "➕ 10주", "callback_data": f"adj_qty_{stk_code}_{10}_{qty}_{entry}_{sl}_{tp}_{strategy_name}"}],
                            [{"text": "🔻 TP -0.5%", "callback_data": f"adj_tp_{stk_code}_{-0.5}_{qty}_{entry}_{sl}_{tp}_{strategy_name}"},
                             {"text": "🔺 TP +0.5%", "callback_data": f"adj_tp_{stk_code}_{0.5}_{qty}_{entry}_{sl}_{tp}_{strategy_name}"}]
                        ]}
                        await send_tg_message(msg, reply_markup=reply_markup)
                        continue

                    if cb_data.startswith('buy_'):
                        parts = cb_data.split('_')
                        if len(parts) >= 5:
                            stock_code, qty, entry, sl = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
                            tp = int(parts[5]) if len(parts) >= 6 else 0
                            stk_name = stock_dict.get(stock_code, stock_code)
                            
                            buy_result, odno = await kiwoom_api.buy_limit_order(session, stock_code, qty, entry)
                            if "✅" in buy_result:
                                await send_tg_message(buy_result)
                                ob_meta = pending_setups.get(stock_code, {})
                                auto_watch_list[stock_code] = {
                                    'name': stk_name, 'entry': entry, 'sl': sl, 'tp': tp, 
                                    'status': 'pending', 'half_sold': False, 'max_reached': entry, 
                                    'odno': odno, 'entry_time': time.time(),
                                    'is_ob_swing': (ob_meta.get('strategy') == 'OB'), 'tf': ob_meta.get('tf', '5m'), 'meta': ob_meta
                                }
                                await save_watch_list(redis_client, auto_watch_list, use_redis)
                                if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(stock_code)
                                watched_stocks.add(stock_code)
                            else:
                                match = re.search(r'(\d+)\s*주\s*매수가능', buy_result)
                                if match and int(match.group(1)) > 0:
                                    max_qty = int(match.group(1))
                                    msg = f"⚠️ [증거금 부족 자동 보정]\n💡 {max_qty}주 (최대) 진입하시겠습니까?"
                                    reply_markup = {"inline_keyboard": [[{"text": f"🚀 {max_qty}주 진입", "callback_data": f"buy_{stock_code}_{max_qty}_{entry}_{sl}_{tp}"}]]}
                                    await send_tg_message(msg, reply_markup=reply_markup)
                                elif any(keyword in buy_result for keyword in ["증거금", "부족", "초과", "가능금액", "잔고"]):
                                    msg = f"⚠️ [매수 증거금 부족]\n💡 수량을 직접 입력하시겠습니까?"
                                    reply_markup = {"inline_keyboard": [[{"text": "✍️ 수량 직접 입력", "callback_data": f"chgqty_{stock_code}_{entry}_{sl}_{tp}"}]]}
                                    await send_tg_message(msg, reply_markup=reply_markup)
                                else:
                                    await send_tg_message(buy_result)
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
                    watched_stocks.clear()
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
                            engine_str = "기타"
                            if cond.get('is_gemini'): engine_str = "제미나이 스캘핑"
                            elif cond.get('is_laptop'): engine_str = "랩탑 스윙"
                            elif cond.get('is_ob_swing'): engine_str = f"{cond.get('meta', {}).get('strategy', 'OB')} 스윙"
                            
                            watch_msg += f"🔹 {cond.get('name', code)} ({engine_str} / {status_str})\n"
                        await send_tg_message(watch_msg)
                    continue

            is_weekend = (now.weekday() >= 5)
            is_active_day = not is_weekend and not is_paused
            
            scan_end_time = dt_time(20, 0) if user_settings.get('nxt_scan_enabled', False) else dt_time(15, 30)
            is_scan_time = (dt_time(9, 0) <= now.time() <= scan_end_time) and is_active_day
            
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

            if is_notify_time and now.minute % 10 == 0 and now.second >= 10 and (current_timestamp - last_auto_chart_time > 60):
                if len(asset_history) >= 2:
                    await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                last_auto_chart_time = current_timestamp

            allow_gem = user_settings.get('engine_gem_on', True)
            allow_lap = user_settings.get('engine_laptop_on', True)
            allow_ob = user_settings.get('engine_ob_on', True)
            allow_bpr = user_settings.get('engine_bpr_on', True)
            
            if user_settings.get('time_filter_on', False):
                if not (9 <= now.hour <= 10 and (now.hour == 9 or now.minute <= 30)):
                    allow_gem, allow_lap = False, False
                if not (13 <= now.hour <= 15 and (now.hour < 15 or now.minute <= 20)):
                    allow_ob, allow_bpr = False, False

            if is_scan_time and (current_timestamp - last_scan_time > 50):
                is_3m = (now.minute % 3 == 0)
                is_5m = (now.minute % 5 == 0)
                
                if is_3m or is_5m:
                    current_macro_pct = await get_kosdaq_macro_trend(session) 
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

                    if target_codes:
                        # 🚨 인메모리 관제 풀 확장 로직
                        new_codes = set(target_codes) - watched_stocks
                        for ncode in new_codes:
                            if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(ncode)
                            past_data = await kiwoom_api.get_candles(session, ncode, '1')
                            if past_data: candle_engine.init_stock(ncode, past_data)
                            watched_stocks.add(ncode)
                            
                        holdings_check = await kiwoom_api.get_holdings_data(session) or {}
                        async def fetch_with_sem(code, tf):
                            async with api_semaphore: return await kiwoom_api.get_candles(session, code, tf)
                                
                        candles_3m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '3') for code in target_codes]))) if is_3m else {}
                        candles_5m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '5') for code in target_codes]))) if is_5m else {}
                        
                        for code in target_codes:
                            if code in holdings_check: continue
                            
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
                                                    auto_watch_list[code] = {'name': stock_dict[code], 'entry': entry, 'sl': gemini_data['sl_price'], 'tp': gemini_data['dynamic_tp'], 'status': 'pending', 'half_sold': False, 'max_reached': entry, 'odno': odno, 'is_gemini': True, 'meta': gemini_data['meta'], 'entry_time': time.time()}
                                                    await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                    if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                    watched_stocks.add(code)
                                                    msg = f"🤖 [제미나이 자동매수] {stock_dict[code]} {qty}주 완료\n"
                                                    msg += f"• 매수가: {entry:,}원\n"
                                                    msg += f"• 목표가: {gemini_data['dynamic_tp']:,}원 (+{user_settings.get('gemini_tp_pct', 1.5)}%)\n"
                                                    msg += f"• 손절가: {gemini_data['sl_price']:,}원 (-{user_settings.get('gemini_sl_pct', 1.0)}%)\n"
                                                    msg += f"• 작동 Lv: {user_settings.get('gemini_filter_lvl', 3)}\n"
                                                    msg += f"• 진단 팩트: {gemini_data['meta'].get('diag_msg', '확인불가')}"
                                                    await send_tg_message(msg)
                                            continue 
                                        
                                    if allow_ob:
                                        is_ob, ob_data = strategy.check_orderblock_engulfing(candles_3m, today_str, user_settings.get('rr_ratio', 1.5))
                                        if is_ob:
                                            ob_data['tf'] = '3m'
                                            ob_data['meta']['macro_pct'] = current_macro_pct 
                                            alert_key = f"{code}_3m_OB_{ob_data['time']}"
                                            if alert_key not in alerted_obs:
                                                alerted_obs.add(alert_key)
                                                await save_alerted_obs(alerted_obs)
                                                pending_setups[code] = ob_data
                                                
                                                ask_v, bid_v, _, _ = await kiwoom_api.get_orderbook(session, code)
                                                opt_entry = strategy.optimize_entry_with_orderbook(ob_data['entry_price'], ask_v, bid_v)
                                                rt_price = abs(int(candles_3m[0]['close']))
                                                
                                                if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(ob_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                    msg = f"🔥 [수동 스윙: OB] (3분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {ob_data['sl_price']:,}원\n• 목표가: {ob_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                    reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_3_{opt_entry}_{ob_data['sl_price']}_{ob_data['dynamic_tp']}_OB"}]]}
                                                    await send_tg_message(msg, reply_markup=reply_markup)

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
                                                    auto_watch_list[code] = {'name': stock_dict[code], 'entry': entry, 'sl': lap_data['sl_price'], 'tp': lap_data['dynamic_tp'], 'status': 'pending', 'half_sold': False, 'max_reached': entry, 'odno': odno, 'is_laptop': True, 'meta': lap_data['meta'], 'entry_time': time.time()}
                                                    await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                    if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                    watched_stocks.add(code)
                                                    await send_tg_message(f"💻 [랩탑 자동매수] {stock_dict[code]} {qty}주 완료 (리스크 동일화 적용)")
                                        continue

                                if allow_bpr:
                                    is_bpr, bpr_data = strategy.check_bpr_ifvg_model(candles_5m, today_str, user_settings.get('rr_ratio', 1.5))
                                    if is_bpr:
                                        bpr_data['tf'] = '5m'
                                        bpr_data['meta']['macro_pct'] = current_macro_pct 
                                        alert_key = f"{code}_5m_BPR_{bpr_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            pending_setups[code] = bpr_data
                                            
                                            ask_v, bid_v, _, _ = await kiwoom_api.get_orderbook(session, code)
                                            opt_entry = strategy.optimize_entry_with_orderbook(bpr_data['entry_price'], ask_v, bid_v)
                                            rt_price = abs(int(candles_5m[0]['close']))
                                            
                                            if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(bpr_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                msg = f"🔥 [수동 스윙: BPR] (5분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {bpr_data['sl_price']:,}원\n• 목표가: {bpr_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_5_{opt_entry}_{bpr_data['sl_price']}_{bpr_data['dynamic_tp']}_BPR"}]]}
                                                await send_tg_message(msg, reply_markup=reply_markup)
                                        continue

                                if allow_ob:
                                    is_ob, ob_data = strategy.check_orderblock_engulfing(candles_5m, today_str, user_settings.get('rr_ratio', 1.5))
                                    if is_ob:
                                        ob_data['tf'] = '5m'
                                        ob_data['meta']['macro_pct'] = current_macro_pct 
                                        alert_key = f"{code}_5m_OB_{ob_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            pending_setups[code] = ob_data
                                            
                                            ask_v, bid_v, _, _ = await kiwoom_api.get_orderbook(session, code)
                                            opt_entry = strategy.optimize_entry_with_orderbook(ob_data['entry_price'], ask_v, bid_v)
                                            rt_price = abs(int(candles_5m[0]['close']))
                                            
                                            if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(ob_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                msg = f"🔥 [수동 스윙: OB] (5분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {ob_data['sl_price']:,}원\n• 목표가: {ob_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_5_{opt_entry}_{ob_data['sl_price']}_{ob_data['dynamic_tp']}_OB"}]]}
                                                await send_tg_message(msg, reply_markup=reply_markup)
                last_scan_time = time.time()

            if is_active_day and auto_watch_list and (current_timestamp - last_monitor_time >= 1):
                state_changed = False
                
                for code, cond in list(auto_watch_list.items()):
                    rt_price = kiwoom_api.realtime_prices.get(code)
                    
                    if not rt_price:
                        async with api_semaphore:
                            c1 = await kiwoom_api.get_candles(session, code, '1')
                        if not c1: continue
                        rt_price = abs(int(c1[0]['close']))
                        
                    # 🚨 신규 연동: 실시간 데이터를 인메모리 캔들에 즉시 갱신
                    candle_engine.update_tick(code, rt_price)

                    stock_name = cond.get('name', code)
                    
                    if cond['status'] == 'pending':
                        cond['status'] = 'active'
                        state_changed = True
                        await send_tg_message(f"🟢 [{stock_name}] 감시 활성화 (웹소켓 연결 완료).")

                    max_reached = cond.get('max_reached', cond['entry'])
                    if rt_price > max_reached: cond['max_reached'] = rt_price

                    if not cond.get('half_sold', False) and not cond.get('be_triggered', False):
                        is_trigger = False
                        if cond.get('is_gemini') and rt_price >= (cond['entry'] * 1.01): is_trigger = True
                        elif (cond.get('is_ob_swing') or cond.get('is_laptop')) and cond['tp'] > 0 and rt_price >= (cond['entry'] + (cond['tp'] - cond['entry'])/2): is_trigger = True
                        
                        if is_trigger:
                            be_sl = strategy.ceil_to_tick(cond['entry'] * 1.003) 
                            if be_sl > cond['sl']:
                                cond['sl'] = be_sl
                                cond['be_triggered'] = True
                                state_changed = True
                    
                    if cond['tp'] > 0 and rt_price >= cond['tp']:
                        if not cond.get('half_sold', False):
                            held_qty = user_settings.get('gemini_amount', 500000) // cond['entry']
                            sell_qty = held_qty // 2
                            if sell_qty == 0: sell_qty = held_qty
                                
                            sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                            if "✅" not in sell_res: sell_res = await kiwoom_api.sell_limit_order(session, code, sell_qty, rt_price)
                            await send_tg_message(f"🚀 [{stock_name}] 타겟 도달! 익절결과: {sell_res}")
                            
                            if "✅" in sell_res:
                                pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                daily_realized_pnl += (rt_price - cond['entry']) * sell_qty
                                hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                mfe_pct = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                meta = cond.get('meta', {})
                                
                                if held_qty - sell_qty > 0:
                                    new_sl = strategy.ceil_to_tick(cond['entry'] * 1.01)
                                    cond.update({'sl': new_sl, 'tp': 0, 'half_sold': True})
                                    state_changed = True
                                    await send_tg_message(f"🛡️ [무한 트레일링] 잔여 물량 추적 시작 (최소보장: {new_sl:,}원)")
                                    reason = "반익절(TP)"
                                else:
                                    del auto_watch_list[code]
                                    # 영구 관제 풀이므로 websocket 해제(unsubscribe) 제거
                                    state_changed = True
                                    reason = "전량익절(TP)"
                                    
                                if cond.get('is_gemini'):
                                    headers = ['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolBurstRatio', 'EntryATR', 'UpperTailRatio', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('upper_tail_ratio', 0), meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                elif cond.get('is_laptop'):
                                    headers = ['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'PullbackVolRatio', 'EntryATR', 'Dummy', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('pullback_vol_ratio', 0), meta.get('entry_atr', 0), 0, meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'laptop_log.csv', headers, row)
                                elif cond.get('is_ob_swing'):
                                    headers = ['Date', 'Code', 'Name', 'Timeframe', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolRatio', 'InitialRR', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('tf', '5m'), cond['entry'], rt_price, pnl_pct, meta.get('vol_ratio', 0), meta.get('actual_rr', 0), meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'ob_log.csv', headers, row)
                            else:
                                if "20" in sell_res or "부족" in sell_res:
                                    await send_tg_message(f"⚠️ [{stock_name}] 수량 부족/미체결 묶임(에러 20) 팩트 확인. 봇 감시를 강제 종료합니다. (HTS/MTS 수동 확인 요망)")
                                    del auto_watch_list[code]
                                else:
                                    cond['tp'] = 0 
                                    await send_tg_message(f"⚠️ [{stock_name}] 통신 에러 누적 방지. 수동 대응 바랍니다.")
                                state_changed = True
                        
                        elif cond.get('half_sold', False) and cond['tp'] == 0:
                            trail_gap = (cond['entry'] * 0.015) 
                            new_sl = max(cond['sl'], strategy.floor_to_tick(cond['max_reached'] - trail_gap))
                            if new_sl > cond['sl']:
                                cond['sl'] = new_sl
                                state_changed = True

                        if cond['sl'] > 0 and rt_price <= cond['sl']:
                            reason_msg = "손절선"
                            if cond.get('half_sold'): reason_msg = "트레일링 방어선"
                            elif cond.get('be_triggered'): reason_msg = "본절 방어선"
                            
                            await send_tg_message(f"🔴 [{stock_name}] {reason_msg} 이탈! 동적 지정가 투매(Dynamic Limit Chaser) 시작.")
                            
                            _, _, _, best_bid = await kiwoom_api.get_orderbook(session, code)
                            target_sell_price = best_bid if best_bid > 0 else rt_price
                            
                            try:
                                tick_size = strategy.get_tick_size(target_sell_price)
                                target_sell_price = target_sell_price - tick_size
                            except:
                                target_sell_price = target_sell_price - 1
                                
                            held_qty = user_settings.get('gemini_amount', 500000) // cond['entry']
                            sell_res = await kiwoom_api.sell_limit_order(session, code, held_qty, target_sell_price)
                            
                            if "✅" not in sell_res:
                                await send_tg_message(f"⚠️ 동적 지정가 거부됨. 플랜 C(시장가) 즉시 투매 시작.")
                                sell_res = await kiwoom_api.sell_market_order(session, code, held_qty)

                            if "✅" in sell_res:
                                await send_tg_message(f"🚩 [{stock_name}] 강제 청산 완료. 결과: {sell_res}")
                                pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                daily_realized_pnl += (rt_price - cond['entry']) * held_qty
                                hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                mfe_pct = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                meta = cond.get('meta', {})
                                
                                if cond.get('half_sold'): reason = "무한트레일링"
                                elif cond.get('be_triggered'): reason = "본절(B/E)"
                                else: reason = "손절(SL)"
                                    
                                if cond.get('is_gemini'):
                                    headers = ['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolBurstRatio', 'EntryATR', 'UpperTailRatio', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('upper_tail_ratio', 0), meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                elif cond.get('is_laptop'):
                                    headers = ['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'PullbackVolRatio', 'EntryATR', 'Dummy', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('pullback_vol_ratio', 0), meta.get('entry_atr', 0), 0, meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'laptop_log.csv', headers, row)
                                elif cond.get('is_ob_swing'):
                                    headers = ['Date', 'Code', 'Name', 'Timeframe', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolRatio', 'InitialRR', 'Macro(%)', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']
                                    row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('tf', '5m'), cond['entry'], rt_price, pnl_pct, meta.get('vol_ratio', 0), meta.get('actual_rr', 0), meta.get('macro_pct', 0.0), hold_sec, mfe_pct, reason]
                                    await asyncio.to_thread(write_trade_log, 'ob_log.csv', headers, row)
                                    
                                del auto_watch_list[code]
                                state_changed = True
                            else:
                                await send_tg_message(f"❌ [{stock_name}] 최종 매도 실패. 좀비 부활 방지를 위해 감시 목록에서 즉시 도려냅니다.\n• 사유: {sell_res}")
                                del auto_watch_list[code]
                                state_changed = True
                    
                if state_changed: await save_watch_list(redis_client, auto_watch_list, use_redis)
                
                if user_settings.get('circuit_breaker_on', False):
                    limit_amt = -(user_settings['risk_amount'] * 3) 
                    if daily_realized_pnl <= limit_amt and not is_paused:
                        is_paused = True
                        await send_tg_message(f"🚨 [서킷 브레이커 발동] 금일 누적 손실({daily_realized_pnl:,}원)이 한도를 초과했습니다. 시드 보호를 위해 봇 전원을 강제 셧다운합니다.")

                last_monitor_time = time.time()
            
            await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())