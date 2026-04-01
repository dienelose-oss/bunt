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

chart_lock = threading.Lock()
KST = timezone(timedelta(hours=9))

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
        'min_tick_diff': 3,
        'max_entry_gap_ticks': 50, 
        'gemini_amount': 500000,
        'gemini_tp_pct': 1.5,
        'gemini_sl_pct': 1.0,
        'nxt_scan_enabled': False,
        'max_holdings': 3,
        'custom_watchlist': {} 
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

# 🚨 [수술 후] 네이버 모바일 API (UTF-8 JSON) 직결 통신으로 한글 깨짐 원천 차단
async def get_stock_name_from_naver(session, code):
    # PC 웹페이지(HTML)가 아닌 모바일 전용 순수 데이터 API 엔드포인트
    url = f"https://m.stock.naver.com/api/stock/{code}/basic"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'
    }
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            data = await resp.json()  # 텍스트가 아닌 JSON으로 깔끔하게 수신
            if data and 'stockName' in data:
                return data['stockName'].strip()
    except Exception as e:
        print(f"네이버 모바일 API 종목명 역산 오류: {e}")
    return None

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
    if len(asset_history) < 2: 
        return False
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

def _save_daily_chart(asset_history, base_amount, date_str):
    if len(asset_history) < 2: 
        return False
    with chart_lock:
        times = [x[0] for x in asset_history]
        assets = [x[1] for x in asset_history]
        plt.figure(figsize=(10, 5))
        line_color = 'red' if assets[-1] >= base_amount else 'blue'
        plt.plot(times, assets, color=line_color, linewidth=2)
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False
        plt.title(f"[{date_str}] 일일 추정자산 마감 흐름")
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xticks(rotation=45)
        plt.tight_layout()
        chart_path = f'asset_chart_{date_str}.png'
        plt.savefig(chart_path)
        plt.close()
    return True

def _generate_and_send_candle_chart(candles, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name):
    if not candles or len(candles) < 10: 
        return False
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
            if bh == 0: 
                bh = strategy.get_tick_size(c['close'])
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
    try: 
        os.remove(chart_path) 
    except: 
        pass

async def send_candle_chart(session, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name):
    candles = await kiwoom_api.get_candles(session, stock_code, tic_scope)
    await asyncio.to_thread(_generate_and_send_candle_chart, candles, stock_name, stock_code, tic_scope, entry_price, sl_price, tp_price, strategy_name)

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
    
    last_cleared_hour = None
    last_daily_reset_date = None
    awaiting_setting = None
    
    is_paused = False
    
    asset_history, max_assets_today, max_assets_time = await asyncio.to_thread(load_today_asset_data)

    async with aiohttp.ClientSession() as session:
        user_settings = await load_or_init_settings(session)
        print(f"\n✅ 시스템 초기화 완료. 백업 관제 {len(auto_watch_list)}개, 알림 로그 {len(alerted_obs)}개 복구.")

        manual_msg = f"""
🤖 [관제탑 3대 퀀트 전술 스캐너 가동]
🔥 1. 제미나이 모멘텀 스캘핑 (자동/3분)
🔥 2. 정통 오더블럭 스윙 (수동/3분+5분)
🔥 3. BPR / IFVG 모델 (수동/5분)

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
                base_saved_for_today = False 
                alerted_obs.clear()
                pending_setups.clear()
                await save_alerted_obs(alerted_obs)
                last_daily_reset_date = today_str
                await send_tg_message("🌅 [07:55] 시스템 메모리 초기화. 금일 데이터를 준비합니다.")
            
            if now.minute == 0 and last_cleared_hour != now.hour:
                os.system('cls' if os.name == 'nt' else 'clear')
                last_cleared_hour = now.hour

            new_commands = await asyncio.to_thread(telegram_bot.fetch_commands)
            
            for cmd in new_commands:
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
                        else:
                            await send_tg_message("❌ 이미 삭제되었거나 존재하지 않는 종목입니다.")
                        continue

                if cmd == 'ㅁ':
                    msg = "🎛️ [메인 관제탑 대시보드]\n원하시는 메뉴를 터치하십시오."
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "📈 관심종목 관리", "callback_data": "menu_watch"}, {"text": "👀 감시 현황", "callback_data": "menu_monitor"}],
                            [{"text": "💰 계좌 잔고", "callback_data": "menu_balance"}, {"text": "📊 누적 통계 분석", "callback_data": "menu_analysis"}],
                            [{"text": "⚙️ 엔진 세팅", "callback_data": "menu_setting"}]
                        ]
                    }
                    await send_tg_message(msg, reply_markup=reply_markup)
                    continue

                if cmd == '중지':
                    is_paused = True
                    await send_tg_message("🛑 [시스템 수동 중지] 휴장일 모드로 전환되어 모든 감시와 API 통신을 차단합니다.")
                    continue
                    
                if cmd == '가동':
                    is_paused = False
                    await send_tg_message("▶️ [시스템 수동 가동] 감시 및 API 통신을 정상 재개합니다.")
                    continue

                if cmd == '분석초기화':
                    if os.path.exists('gemini_log.csv'):
                        os.rename('gemini_log.csv', f'gemini_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                    if os.path.exists('ob_log.csv'):
                        os.rename('ob_log.csv', f'ob_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                    await send_tg_message(f"♻️ 딥러닝 누적 데이터가 백업/초기화되었습니다.")
                    continue
                    
                if cmd == '분석':
                    if not os.path.exists('gemini_log.csv') and not os.path.exists('ob_log.csv'):
                        await send_tg_message("❌ 분석할 데이터가 아직 존재하지 않습니다.")
                        continue
                        
                    msg = "📊 [퀀트 누적 통계 분석 리포트]\n"
                    
                    def _build_report(file_path, engine_name, base_amt):
                        if not os.path.exists(file_path): return ""
                        try:
                            df = pd.read_csv(file_path)
                            if len(df) == 0: return f"\n[{engine_name}] 기록된 데이터가 없습니다.\n"
                            
                            def _categorize(row):
                                reason = str(row.get('ExitReason', ''))
                                pnl = float(row.get('PnL(%)', 0))
                                if '익절' in reason or (reason == '트레일링' and pnl > 0): return 'Win'
                                elif '본절' in reason or (reason == '트레일링' and pnl == 0): return 'Draw'
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
                            rep += f"• 추정 실현손익: {int(total_pnl):,}원\n  (1회 {base_amt:,}원 세팅 기준)\n"
                            rep += f"• 평균 익절: +{avg_win:.2f}% / 평균 손절: {avg_loss:.2f}%\n"
                            
                            rep += f"💡 [시스템 세팅 진단]\n"
                            if win_rate >= 45.0:
                                rep += "현재 설정이 시장에 완벽히 피팅되어 있습니다. 현행 유지를 권장합니다.\n"
                            else:
                                rec_tp = round(max(avg_win, 1.2), 1) if avg_win > 0 else 1.5
                                rec_sl = round(abs(avg_loss), 1) if avg_loss < 0 else 1.0
                                rep += f"익절 +{rec_tp}%, 손절 -{rec_sl}% 로 미세 조정 테스트를 권장합니다.\n"
                            return rep
                        except Exception as e:
                            return f"\n❌ {engine_name} 분석 오류: {e}\n"

                    msg += _build_report('gemini_log.csv', '제미나이 스캘핑', user_settings.get('gemini_amount', 500000))
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
                            
                        # 🚨 [팩트 반영] 엔진 진단 시 today_str 인자 동기화
                        is_gem, gem_data = strategy.check_gemini_momentum_model(c3, today_str, tp_pct=user_settings.get('gemini_tp_pct', 1.5), sl_pct=user_settings.get('gemini_sl_pct', 1.0))
                        msg += f"🤖 제미나이(3m): {'✅ 타점 포착 ('+str(gem_data.get('entry_price'))+'원)' if is_gem else '❌ 조건 불충족'}\n"
                        
                        is_ob3, ob3_data = strategy.check_orderblock_engulfing(c3, today_str, user_settings.get('rr_ratio', 1.5))
                        msg += f"🎯 OB 스윙(3m): {'✅ 타점 포착 ('+str(ob3_data.get('entry_price'))+'원)' if is_ob3 else '❌ 조건 불충족'}\n"
                        
                        is_bpr, bpr_data = strategy.check_bpr_ifvg_model(c5, today_str, user_settings.get('rr_ratio', 1.5))
                        msg += f"🔥 BPR 스윙(5m): {'✅ 타점 포착 ('+str(bpr_data.get('entry_price'))+'원)' if is_bpr else '❌ 조건 불충족'}\n"
                        
                        is_ob5, ob5_data = strategy.check_orderblock_engulfing(c5, today_str, user_settings.get('rr_ratio', 1.5))
                        msg += f"🎯 OB 스윙(5m): {'✅ 타점 포착 ('+str(ob5_data.get('entry_price'))+'원)' if is_ob5 else '❌ 조건 불충족'}\n\n"
                        msg += "💡 (엔진 수식 연산 100% 정상 작동 중)"
                        
                        await send_tg_message(msg)
                    else:
                        await send_tg_message("❌ 실시간 검색 상위 종목 수신 실패")
                    continue

                if awaiting_setting:
                    if awaiting_setting == 'watch_add':
                        code = cmd.strip()
                        if len(code) == 6 and code.isdigit():
                            watchlist = user_settings.setdefault('custom_watchlist', {})
                            if code in watchlist:
                                await send_tg_message("❌ 이미 등록된 종목입니다.")
                            elif len(watchlist) >= 10:
                                await send_tg_message("❌ 관심종목은 트래픽 방어를 위해 최대 10개까지만 등록 가능합니다.")
                            else:
                                await send_tg_message(f"🔍 [{code}] 네이버 금융 API로 공식 종목명을 역산 중입니다...")
                                name = await get_stock_name_from_naver(session, code)
                                if name:
                                    watchlist[code] = name
                                    def _save_set():
                                        with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                    await asyncio.to_thread(_save_set)
                                    await send_tg_message(f"✅ 관심종목 [{name} ({code})] 추가 및 엔진 락온 완료.")
                                else:
                                    await send_tg_message("❌ 종목명을 찾을 수 없습니다. 상장폐지 또는 잘못된 코드입니다.")
                        else:
                            await send_tg_message("❌ 올바른 6자리 종목코드를 입력하세요.")
                        awaiting_setting = None
                        continue

                    elif awaiting_setting.startswith('customtp_'):
                        parts = awaiting_setting.split('_')
                        stk_code, qty, entry, sl = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
                        try:
                            new_tp = strategy.floor_to_tick(int(cmd.replace(',', '').strip()))
                            if new_tp <= entry:
                                await send_tg_message("❌ 목표가는 진입가보다 높아야 합니다.")
                            else:
                                stk_name = stock_dict.get(stk_code, stk_code)
                                await send_candle_chart(session, stk_name, stk_code, '5', entry, sl, new_tp, "수동 목표가")
                                msg = f"🎯 [{stk_name} 수동 튜닝 산출]\n• 대기: {entry:,}원\n• 손절: {sl:,}원\n• 익절: {new_tp:,}원\n• 수량: {qty:,}주"
                                reply_markup = {"inline_keyboard": [[{"text": f"🚀 {qty}주 즉시 진입 및 감시", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{new_tp}"}]]}
                                await send_tg_message(msg, reply_markup=reply_markup)
                        except ValueError: await send_tg_message("❌ 올바른 숫자가 아닙니다.")
                        finally: awaiting_setting = None
                        continue
                    
                    elif awaiting_setting.startswith('manualqty_'):
                        parts = awaiting_setting.split('_')
                        stk_code, entry, sl, tp = parts[1], int(parts[2]), int(parts[3]), int(parts[4])
                        try:
                            qty = int(cmd.replace(',', '').strip())
                            if qty <= 0:
                                await send_tg_message("❌ 수량은 1주 이상이어야 합니다.")
                            else:
                                stk_name = stock_dict.get(stk_code, stk_code)
                                msg = f"🎯 [{stk_name} 부분 수량 진입]\n• 매수가: {entry:,}원\n• 보정 수량: {qty:,}주\n해당 수량으로 강제 진입을 시도합니다."
                                reply_markup = {"inline_keyboard": [[{"text": f"🚀 {qty}주 강제 진입", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{tp}"}]]}
                                await send_tg_message(msg, reply_markup=reply_markup)
                        except ValueError: await send_tg_message("❌ 올바른 숫자가 아닙니다. 취소합니다.")
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
                    nxt_status = "🟢 ON (08:00~20:00)" if user_settings.get('nxt_scan_enabled', False) else "🔴 OFF (09:00~15:30)"
                    
                    msg = f"⚙️ [시스템 세팅 상태]\n\n"
                    msg += f"📘 포트폴리오 및 수동 진입 설정\n"
                    msg += f"• 포트폴리오 최대 한도: {user_settings.get('max_holdings', 3)}종목\n"
                    msg += f"• 1회 허용 손실비용: {user_settings['risk_amount']:,}원\n"
                    msg += f"• 기본 손익비 (R:R): 1:{user_settings['rr_ratio']}\n"
                    msg += f"• 스윕-진입 최소 틱 필터: {user_settings.get('min_tick_diff', 3)}틱\n"
                    msg += f"• 최대 진입 이격 허용: {user_settings.get('max_entry_gap_ticks', 50)}틱 제한\n\n"
                    msg += f"🔥 자동 스캘핑 설정\n"
                    msg += f"• 1회 고정 진입금액: {user_settings.get('gemini_amount', 500000):,}원\n"
                    msg += f"• 자동 익절: +{user_settings.get('gemini_tp_pct', 1.5)}%\n"
                    msg += f"• 자동 손절: -{user_settings.get('gemini_sl_pct', 1.0)}%\n\n"
                    msg += f"🌌 NXT 연장장 스캔 모드: {nxt_status}\n\n"
                    msg += f"변경할 항목을 아래에서 선택하세요."
                    
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "포트폴리오 한도", "callback_data": "set_maxholdings"}, {"text": "수동 손실비용", "callback_data": "set_risk"}],
                            [{"text": "수동 손익비", "callback_data": "set_rr"}, {"text": "자동 진입금액", "callback_data": "set_gemini_amount"}],
                            [{"text": "자동 익절%", "callback_data": "set_gemini_tp"}, {"text": "자동 손절%", "callback_data": "set_gemini_sl"}],
                            [{"text": "최소 틱 필터", "callback_data": "set_mintick"}, {"text": "최대 이격 틱수", "callback_data": "set_maxgap"}],
                            [{"text": f"NXT 스캔 변경", "callback_data": "toggle_nxt"}, {"text": "기준 자산 갱신", "callback_data": "set_base"}]
                        ]
                    }
                    await send_tg_message(msg, reply_markup=reply_markup)
                    continue

                if cmd.startswith('cb:'):
                    cb_data = cmd.replace('cb:', '')
                    
                    if cb_data == 'toggle_nxt':
                        user_settings['nxt_scan_enabled'] = not user_settings.get('nxt_scan_enabled', False)
                        def _save_set():
                            with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                        await asyncio.to_thread(_save_set)
                        new_status = "ON (연장장 포함)" if user_settings['nxt_scan_enabled'] else "OFF (정규장 전용)"
                        await send_tg_message(f"✅ NXT 연장장 스캔 모드가 **{new_status}** 되었습니다.")
                        continue
                    
                    if cb_data in ['set_base', 'set_risk', 'set_rr', 'set_mintick', 'set_maxgap', 'set_gemini_amount', 'set_gemini_tp', 'set_gemini_sl', 'set_maxholdings']:
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
                        await send_tg_message(f"✏️ [{stk_name}] HTS/MTS에서 확인한 **현재 살 수 있는 최대 수량(주)**을 숫자로 입력하세요:")
                        continue

                    if cb_data.startswith('delwatch_'):
                        code_to_del = cb_data.split('_')[1]
                        if code_to_del in auto_watch_list:
                            target_cond = auto_watch_list[code_to_del]
                            stk_name = target_cond.get('name', code_to_del)
                            
                            cancel_msg = ""
                            if target_cond.get('status') == 'pending':
                                old_odno = target_cond.get('odno')
                                if old_odno:
                                    cancel_res = await kiwoom_api.cancel_order(session, code_to_del, old_odno)
                                    cancel_msg = f"\n(미체결 주문 동기화 취소: {cancel_res})"
                                else:
                                    cancel_msg = "\n(⚠️ 취소 실패: 저장된 원주문번호가 없습니다)"
                                
                            del auto_watch_list[code_to_del]
                            await save_watch_list(redis_client, auto_watch_list, use_redis)
                            await send_tg_message(f"🗑️ [{stk_name}] 개별 감시가 취소되었습니다.{cancel_msg}")
                        else:
                            await send_tg_message("❌ 이미 감시 목록에 없는 종목입니다.")
                        continue

                    if cb_data.startswith('obcalc_'):
                        parts = cb_data.split('_')
                        if len(parts) < 6: continue
                        stk_code, tic, entry, sl, tp = parts[1], parts[2], int(parts[3]), int(parts[4]), int(parts[5])
                        strategy_name = parts[6] if len(parts) >= 7 else "수동 산출"
                        
                        risk_share = entry - sl
                        qty = user_settings['risk_amount'] // risk_share if risk_share > 0 else 0
                        if qty == 0:
                            await send_tg_message("❌ 매수 불가: 설정된 손실비용으로 1주도 살 수 없습니다.")
                            continue
                            
                        stk_name = stock_dict.get(stk_code, stk_code)
                        await send_candle_chart(session, stk_name, stk_code, tic, entry, sl, tp, strategy_name)
                        
                        rt_candles = await kiwoom_api.get_candles(session, stk_code, '1')
                        current_price = abs(int(rt_candles[0]['close'])) if rt_candles else entry
                        
                        tick_diff = strategy.count_exact_ticks(current_price, entry)
                        sign_str = "+" if entry > current_price else "-"
                        
                        msg = f"🎯 [{stk_name} 수동 진입 승인 대기]\n"
                        
                        dynamic_max = user_settings.get('max_holdings', 3)
                        if len(auto_watch_list) >= dynamic_max:
                            msg += f"⚠️ [경고] 현재 포트폴리오 한도({dynamic_max}종목) 초과 상태입니다.\n"
                            
                        msg += f"• 엔진: {strategy_name}\n"
                        msg += f"• 현재가: {current_price:,}원\n"
                        msg += f"• 대기(매수): {entry:,}원 (현재가 대비 {sign_str}{tick_diff}틱)\n"
                        msg += f"• 손절: {sl:,}원\n"
                        msg += f"• 목표: {tp:,}원\n"
                        msg += f"• 산출 수량: {qty:,}주"
                        
                        reply_markup = {
                            "inline_keyboard": [
                                [{"text": f"🚀 {qty}주 승인 (진입)", "callback_data": f"buy_{stk_code}_{qty}_{entry}_{sl}_{tp}"}],
                                [{"text": "🎯 익절가 변경", "callback_data": f"chgtp_{stk_code}_{qty}_{entry}_{sl}"}]
                            ]
                        }
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
                                is_ob_swing = (ob_meta.get('strategy') == 'OB')
                                tf = ob_meta.get('tf', '5m')
                                
                                auto_watch_list[stock_code] = {
                                    'name': stk_name, 'entry': entry, 'sl': sl, 'tp': tp, 
                                    'status': 'pending', 'half_sold': False, 'max_reached': entry, 
                                    'odno': odno, 'entry_time': time.time(),
                                    'is_ob_swing': is_ob_swing, 'tf': tf, 'meta': ob_meta
                                }
                                await save_watch_list(redis_client, auto_watch_list, use_redis)
                            else:
                                match = re.search(r'(\d+)\s*주\s*매수가능', buy_result)
                                if match and int(match.group(1)) > 0:
                                    max_qty = int(match.group(1))
                                    msg = f"⚠️ [증거금 부족 자동 보정]\n• 산출 수량: {qty}주\n• 가능 수량: {max_qty}주\n💡 최대 수량으로 부분 진입하시겠습니까?"
                                    reply_markup = {"inline_keyboard": [[{"text": f"🚀 {max_qty}주 (최대) 진입", "callback_data": f"buy_{stock_code}_{max_qty}_{entry}_{sl}_{tp}"}]]}
                                    await send_tg_message(msg, reply_markup=reply_markup)
                                elif any(keyword in buy_result for keyword in ["증거금", "부족", "초과", "가능금액", "잔고"]):
                                    msg = f"⚠️ [매수 증거금 부족]\n• 사유: {buy_result.replace('❌ ', '')}\n💡 수량을 직접 입력하시겠습니까?"
                                    reply_markup = {"inline_keyboard": [[{"text": "✍️ 수량 직접 입력", "callback_data": f"chgqty_{stock_code}_{entry}_{sl}_{tp}"}]]}
                                    await send_tg_message(msg, reply_markup=reply_markup)
                                else:
                                    await send_tg_message(buy_result)
                        continue
                
                if cmd == '잔고':
                    bal = await kiwoom_api.get_account_balance(session)
                    if bal: await send_tg_message(bal.replace('📊 ', '').replace('📋 ', ''))
                    continue
                    
                if cmd in ['그래프', '차트', 'ㄱ']:
                    await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                    continue

                if cmd == '감시취소':
                    cancel_count = 0
                    for code, cond in list(auto_watch_list.items()):
                        if cond.get('status') == 'pending' and cond.get('odno'):
                            await kiwoom_api.cancel_order(session, code, cond['odno'])
                            cancel_count += 1
                            
                    auto_watch_list.clear()
                    await save_watch_list(redis_client, auto_watch_list, use_redis)
                    await send_tg_message(f"🛑 전체 관제 취소 완료. (미체결 동기화 취소: {cancel_count}건)")
                    continue

                if cmd == '감시':
                    if not auto_watch_list: 
                        await send_tg_message("현재 자동감시 없음.")
                    else:
                        watch_msg = "👀 [현재 감시 현황]\n\n"
                        keyboard = []
                        for code, cond in auto_watch_list.items():
                            rt_candles = await kiwoom_api.get_candles(session, code, '1')
                            current_price = abs(int(rt_candles[0]['close'])) if rt_candles else 0
                            
                            status_str = "🟢 활성" if cond['status'] == 'active' else "⏳ 대기"
                            
                            engine_str = "수동/기타"
                            if cond.get('is_gemini'): engine_str = "제미나이 스캘핑 (3m)"
                            elif cond.get('is_ob_swing'):
                                stype = cond.get('meta', {}).get('strategy', 'OB')
                                tf = cond.get('tf', '?m')
                                engine_str = f"{stype} 스윙 ({tf})"

                            tp_str = f"{cond['tp']:,}원" 
                            if cond['tp'] > 0 and current_price > 0:
                                tp_ticks = strategy.count_exact_ticks(cond['tp'], current_price)
                                tp_sign = "+" if cond['tp'] >= current_price else "-"
                                tp_str += f" ({tp_sign}{tp_ticks}틱)"
                            elif cond['tp'] == 0:
                                tp_str = "트레일링 (추적 상향 중)"
                                
                            sl_str = f"{cond['sl']:,}원"
                            if cond['sl'] > 0 and current_price > 0:
                                sl_ticks = strategy.count_exact_ticks(current_price, cond['sl'])
                                sl_sign = "+" if cond['sl'] >= current_price else "-"
                                sl_str += f" ({sl_sign}{sl_ticks}틱)"
                            if cond.get('half_sold', False): sl_str += " (상향)"
                                
                            curr_str = f"{current_price:,}원" if current_price > 0 else "조회실패"
                                
                            watch_msg += f"🔹 {cond.get('name', code)} ({code})\n"
                            watch_msg += f"• 엔진: {engine_str}\n"
                            watch_msg += f"• 상태: {status_str}\n"
                            watch_msg += f"• 현재가: {curr_str}\n"
                            watch_msg += f"• 대기: {cond['entry']:,}원\n"
                            watch_msg += f"• 목표: {tp_str}\n"
                            watch_msg += f"• 손절: {sl_str}\n\n"
                            
                            keyboard.append([{"text": f"❌ {cond.get('name', code)} 감시 취소", "callback_data": f"delwatch_{code}"}])
                            
                        await send_tg_message(watch_msg, reply_markup={"inline_keyboard": keyboard})
                    continue

            is_weekend = (now.weekday() >= 5)
            is_active_day = not is_weekend and not is_paused
            is_notify_time = (dt_time(8, 0) <= now.time() <= dt_time(20, 0)) and is_active_day
            
            if user_settings.get('nxt_scan_enabled', False):
                is_scan_time = (dt_time(8, 0) <= now.time() <= dt_time(20, 0)) and is_active_day
            else:
                is_scan_time = (dt_time(9, 0) <= now.time() <= dt_time(15, 30)) and is_active_day
            
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

            # 🚨 [팩트 반영] 엔진 스캔 시 인자 동기화 (today_str 패스)
            if is_scan_time and (current_timestamp - last_scan_time > 50):
                is_3m = (now.minute % 3 == 0)
                is_5m = (now.minute % 5 == 0)
                
                if is_3m or is_5m:
                    top_20 = await kiwoom_api.get_top_20_search_rank(session)
                    
                    if is_3m: print(f"📡 [{now.strftime('%H:%M:%S')}] 3분봉 스캔 루프 작동 중")
                    if is_5m: print(f"📡 [{now.strftime('%H:%M:%S')}] 5분봉 스캔 루프 작동 중")
                    
                    if top_20 is not None:
                        target_codes = list(top_20.keys())
                        stock_dict.update(top_20)
                        
                        watchlist = user_settings.get('custom_watchlist', {})
                        for wc_code, wc_name in watchlist.items():
                            if wc_code not in target_codes:
                                target_codes.append(wc_code)
                                stock_dict[wc_code] = wc_name

                        if target_codes:
                            holdings_check = await kiwoom_api.get_holdings_data(session) or {}
                            
                            async def fetch_with_sem(code, tf):
                                async with api_semaphore: return await kiwoom_api.get_candles(session, code, tf)
                                    
                            candles_3m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '3') for code in target_codes]))) if is_3m else {}
                            candles_5m_dict = dict(zip(target_codes, await asyncio.gather(*[fetch_with_sem(code, '5') for code in target_codes]))) if is_5m else {}
                            
                            for code in target_codes:
                                if code in holdings_check: continue
                                    
                                if is_3m and code in candles_3m_dict:
                                    candles_3m = candles_3m_dict[code]
                                    
                                    if not candles_3m or not candles_3m[0]['time'].startswith(today_str):
                                        continue
                                    
                                    # 🚨 [팩트 주입] today_str 추가 전달
                                    is_gemini, gemini_data = strategy.check_gemini_momentum_model(candles_3m, today_str, tp_pct=user_settings.get('gemini_tp_pct', 1.5), sl_pct=user_settings.get('gemini_sl_pct', 1.0))
                                    
                                    if is_gemini:
                                        alert_key = f"{code}_3m_GEMINI_{gemini_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            
                                            if code in auto_watch_list and auto_watch_list[code]['status'] == 'pending':
                                                if auto_watch_list[code].get('odno'): await kiwoom_api.cancel_order(session, code, auto_watch_list[code]['odno'])
                                                del auto_watch_list[code]
                                                await save_watch_list(redis_client, auto_watch_list, use_redis)

                                            entry = gemini_data['entry_price']
                                            qty = user_settings.get('gemini_amount', 500000) // entry
                                            if qty > 0:
                                                buy_msg = f"⚡ [제미나이 스캘핑 (3분)]\n• 종목: {stock_dict[code]}\n• 폭발 감지! 즉각 자동 진입을 시도합니다."
                                                await send_tg_message(buy_msg)
                                                
                                                buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, entry)
                                                if "✅" in buy_res:
                                                    auto_watch_list[code] = {'name': stock_dict[code], 'entry': entry, 'sl': gemini_data['sl_price'], 'tp': gemini_data['dynamic_tp'], 'status': 'pending', 'half_sold': False, 'max_reached': entry, 'odno': odno, 'is_gemini': True, 'meta': gemini_data['meta'], 'entry_time': time.time()}
                                                    await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                    await send_tg_message(f"🤖 [자동 매수 완료] {stock_dict[code]} {qty}주 / 목표: {gemini_data['dynamic_tp']:,}원")
                                        continue 
                                        
                                    is_ob, ob_data = strategy.check_orderblock_engulfing(candles_3m, today_str, user_settings.get('rr_ratio', 1.5))
                                    if is_ob:
                                        ob_data['tf'] = '3m'
                                        alert_key = f"{code}_3m_OB_{ob_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            pending_setups[code] = ob_data
                                            
                                            ask_v, bid_v = await kiwoom_api.get_orderbook(session, code)
                                            opt_entry = strategy.optimize_entry_with_orderbook(ob_data['entry_price'], ask_v, bid_v)
                                            rt_price = abs(int(candles_3m[0]['close']))
                                            
                                            if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(ob_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                msg = f"🔥 [수동 스윙: OB] (3분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {ob_data['sl_price']:,}원\n• 목표가: {ob_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_3_{opt_entry}_{ob_data['sl_price']}_{ob_data['dynamic_tp']}_OB"}]]}
                                                await send_tg_message(msg, reply_markup=reply_markup)

                                if is_5m and code in candles_5m_dict:
                                    candles_5m = candles_5m_dict[code]
                                    
                                    if not candles_5m or not candles_5m[0]['time'].startswith(today_str):
                                        continue

                                    is_bpr, bpr_data = strategy.check_bpr_ifvg_model(candles_5m, today_str, user_settings.get('rr_ratio', 1.5))
                                    if is_bpr:
                                        bpr_data['tf'] = '5m'
                                        bpr_data['strategy'] = 'BPR'
                                        alert_key = f"{code}_5m_BPR_{bpr_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            pending_setups[code] = bpr_data
                                            
                                            ask_v, bid_v = await kiwoom_api.get_orderbook(session, code)
                                            opt_entry = strategy.optimize_entry_with_orderbook(bpr_data['entry_price'], ask_v, bid_v)
                                            rt_price = abs(int(candles_5m[0]['close']))
                                            
                                            if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(bpr_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                msg = f"🔥 [수동 스윙: BPR] (5분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {bpr_data['sl_price']:,}원\n• 목표가: {bpr_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_5_{opt_entry}_{bpr_data['sl_price']}_{bpr_data['dynamic_tp']}_BPR"}]]}
                                                await send_tg_message(msg, reply_markup=reply_markup)
                                        continue

                                    is_ob, ob_data = strategy.check_orderblock_engulfing(candles_5m, today_str, user_settings.get('rr_ratio', 1.5))
                                    if is_ob:
                                        ob_data['tf'] = '5m'
                                        alert_key = f"{code}_5m_OB_{ob_data['time']}"
                                        if alert_key not in alerted_obs:
                                            alerted_obs.add(alert_key)
                                            await save_alerted_obs(alerted_obs)
                                            pending_setups[code] = ob_data
                                            
                                            ask_v, bid_v = await kiwoom_api.get_orderbook(session, code)
                                            opt_entry = strategy.optimize_entry_with_orderbook(ob_data['entry_price'], ask_v, bid_v)
                                            rt_price = abs(int(candles_5m[0]['close']))
                                            
                                            if strategy.count_exact_ticks(rt_price, opt_entry) <= user_settings.get('max_entry_gap_ticks', 50) and strategy.count_exact_ticks(ob_data['sl_price'], opt_entry) >= user_settings.get('min_tick_diff', 3):
                                                msg = f"🔥 [수동 스윙: OB] (5분봉)\n• 종목: {stock_dict[code]}\n• 현재가: {rt_price:,}원\n• 매수가: {opt_entry:,}원\n• 손절가: {ob_data['sl_price']:,}원\n• 목표가: {ob_data['dynamic_tp']:,}원\n*(진입 승인 대기)*"
                                                reply_markup = {"inline_keyboard": [[{"text": f"⚡ 수동 타점 확인", "callback_data": f"obcalc_{code}_5_{opt_entry}_{ob_data['sl_price']}_{ob_data['dynamic_tp']}_OB"}]]}
                                                await send_tg_message(msg, reply_markup=reply_markup)
                last_scan_time = time.time()

            if is_active_day and auto_watch_list and (current_timestamp - last_monitor_time >= 1):
                holdings = await kiwoom_api.get_holdings_data(session)
                state_changed = False
                
                if holdings is not None:
                    async def fetch_rt(code):
                        async with api_semaphore: return await kiwoom_api.get_candles(session, code, '1')
                    realtime_candles = await asyncio.gather(*[fetch_rt(c) for c in auto_watch_list.keys()])
                    
                    for (code, cond), candles in zip(list(auto_watch_list.items()), realtime_candles):
                        if not candles: continue
                        rt_price = abs(int(candles[0]['close']))
                        
                        if code in holdings:
                            stock_name = holdings[code]['name']
                            held_qty = holdings[code]['qty']
                            
                            if cond['status'] == 'pending':
                                cond['status'] = 'active'
                                cond['name'] = stock_name
                                state_changed = True
                                await send_tg_message(f"🟢 [{stock_name}] 잔고 편입. 실시간 감시 시작.")

                            max_reached = cond.get('max_reached', cond['entry'])
                            if rt_price > max_reached: cond['max_reached'] = rt_price

                            if not cond.get('half_sold', False) and not cond.get('be_triggered', False):
                                is_trigger = False
                                if cond.get('is_gemini') and rt_price >= (cond['entry'] * 1.01): is_trigger = True
                                elif cond.get('is_ob_swing') and cond['tp'] > 0 and rt_price >= (cond['entry'] + (cond['tp'] - cond['entry'])/2): is_trigger = True
                                
                                if is_trigger:
                                    be_sl = strategy.ceil_to_tick(cond['entry'] * 1.003) 
                                    if be_sl > cond['sl']:
                                        cond['sl'] = be_sl
                                        cond['be_triggered'] = True
                                        state_changed = True
                                        await send_tg_message(f"🛡️ [{stock_name}] 방어선 발동! 본절({be_sl:,}원) 상향 (Risk-Free)")
                            
                            if cond['tp'] > 0 and rt_price >= cond['tp']:
                                if not cond.get('half_sold', False):
                                    sell_qty = held_qty // 2
                                    if sell_qty == 0: sell_qty = held_qty
                                        
                                    await send_tg_message(f"🚀 [{stock_name}] 타겟 도달! 시장가 절반 청산 시도.")
                                    sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                                    if "✅" not in sell_res: sell_res = await kiwoom_api.sell_limit_order(session, code, sell_qty, rt_price)
                                    await send_tg_message(sell_res)
                                    
                                    if "✅" in sell_res:
                                        pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                        hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                        mfe_pct = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                        meta = cond.get('meta', {})
                                        
                                        if held_qty - sell_qty > 0:
                                            new_sl = rt_price - (5 * strategy.get_tick_size(rt_price))
                                            cond.update({'sl': new_sl, 'tp': 0, 'half_sold': True})
                                            state_changed = True
                                            await send_tg_message(f"🛡️ [트레일링 스탑] 잔여 물량 추적 시작: {new_sl:,}원")
                                            reason = "반익절(TP)"
                                        else:
                                            del auto_watch_list[code]
                                            state_changed = True
                                            reason = "전량익절(TP)"
                                            
                                        if cond.get('is_gemini'):
                                            log_row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('upper_tail_ratio', 0), hold_sec, mfe_pct, reason]
                                            await asyncio.to_thread(lambda: csv.writer(open('gemini_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row) if os.path.isfile('gemini_log.csv') else [csv.writer(open('gemini_log.csv', 'w', newline='', encoding='utf-8-sig')).writerow(['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolBurstRatio', 'EntryATR', 'UpperTailRatio', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']), csv.writer(open('gemini_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row)])
                                        elif cond.get('is_ob_swing'):
                                            log_row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('tf', '5m'), cond['entry'], rt_price, pnl_pct, meta.get('vol_ratio', 0), meta.get('actual_rr', 0), hold_sec, mfe_pct, reason]
                                            await asyncio.to_thread(lambda: csv.writer(open('ob_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row) if os.path.isfile('ob_log.csv') else [csv.writer(open('ob_log.csv', 'w', newline='', encoding='utf-8-sig')).writerow(['Date', 'Code', 'Name', 'Timeframe', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolRatio', 'InitialRR', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']), csv.writer(open('ob_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row)])
                            
                            elif cond.get('half_sold', False) and cond['tp'] == 0:
                                new_sl, new_max = strategy.calculate_trailing_stop(rt_price, cond['sl'], cond['max_reached'])
                                if new_sl > cond['sl']:
                                    cond['sl'] = new_sl
                                    state_changed = True

                            if cond['sl'] > 0 and rt_price <= cond['sl']:
                                reason_msg = "손절선"
                                if cond.get('half_sold'): reason_msg = "트레일링 방어선"
                                elif cond.get('be_triggered'): reason_msg = "본절 방어선"
                                
                                await send_tg_message(f"🔴 [{stock_name}] {reason_msg} 이탈({rt_price:,}원)! 전량 청산.")
                                sell_res = await kiwoom_api.sell_market_order(session, code, held_qty)
                                if "✅" not in sell_res:
                                    panic_price = rt_price - (5 * strategy.get_tick_size(rt_price))
                                    sell_res = await kiwoom_api.sell_limit_order(session, code, held_qty, max(panic_price, 1))
                                await send_tg_message(sell_res)
                                
                                if "✅" in sell_res:
                                    pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                    hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                    mfe_pct = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                    meta = cond.get('meta', {})
                                    
                                    if cond.get('half_sold'): reason = "트레일링"
                                    elif cond.get('be_triggered'): reason = "본절(B/E)"
                                    else: reason = "손절(SL)"
                                        
                                    if cond.get('is_gemini'):
                                        log_row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond['entry'], rt_price, pnl_pct, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('upper_tail_ratio', 0), hold_sec, mfe_pct, reason]
                                        await asyncio.to_thread(lambda: csv.writer(open('gemini_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row) if os.path.isfile('gemini_log.csv') else [csv.writer(open('gemini_log.csv', 'w', newline='', encoding='utf-8-sig')).writerow(['Date', 'Code', 'Name', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolBurstRatio', 'EntryATR', 'UpperTailRatio', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']), csv.writer(open('gemini_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row)])
                                    elif cond.get('is_ob_swing'):
                                        log_row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('tf', '5m'), cond['entry'], rt_price, pnl_pct, meta.get('vol_ratio', 0), meta.get('actual_rr', 0), hold_sec, mfe_pct, reason]
                                        await asyncio.to_thread(lambda: csv.writer(open('ob_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row) if os.path.isfile('ob_log.csv') else [csv.writer(open('ob_log.csv', 'w', newline='', encoding='utf-8-sig')).writerow(['Date', 'Code', 'Name', 'Timeframe', 'EntryPrice', 'ExitPrice', 'PnL(%)', 'VolRatio', 'InitialRR', 'HoldTime(s)', 'MaxPnL(%)', 'ExitReason']), csv.writer(open('ob_log.csv', 'a', newline='', encoding='utf-8-sig')).writerow(log_row)])
                                        
                                    del auto_watch_list[code]
                                    state_changed = True
                        else:
                            if cond['status'] == 'active':
                                await send_tg_message(f"👋 [{cond.get('name', code)}] 잔고 제외 감지. 감시 종료.")
                                del auto_watch_list[code]
                                state_changed = True
                                
                if state_changed: await save_watch_list(redis_client, auto_watch_list, use_redis)
                last_monitor_time = time.time()
            
            await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())