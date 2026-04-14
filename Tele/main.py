import os
import time
import csv
import json
import asyncio
import aiohttp
import re
import shutil
import traceback
import pandas as pd
import redis.asyncio as redis
from datetime import datetime, time as dt_time, timezone, timedelta 

import kiwoom_api
import telegram_bot
import strategy

# 분리된 서브 모듈에서 유틸리티 함수 및 상수 일괄 임포트
from main_sub import (
    ASSET_FILE, SETTINGS_FILE, STATE_FILE, ALERTED_OBS_FILE, 
    REDIS_WATCH_KEY, SNAPSHOT_FILE, HOLIDAY_FILE, chart_lock, KST, 
    _STOCK_MARKET_CACHE, _get_tick_size, get_d_minus_2_date, 
    get_remaining_trading_days, load_persistent_state, save_watch_list, 
    save_alerted_obs, send_tg_message, load_or_init_settings, 
    get_stock_info_from_naver, get_kosdaq_macro_trend, 
    get_macro_minute_candles, load_today_asset_data, 
    _generate_and_send_asset_chart, write_trade_log, get_time_session
)

async def main():
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auto_watch_list, alerted_obs, use_redis = await load_persistent_state(redis_client)
    api_semaphore = asyncio.Semaphore(4)

    stock_dict = {}
    pending_setups = {} 
    accumulated_targets = {}  # 🚨 FIFO 누적 추적용 딕셔너리
    
    max_profit_today = 0  # 🚨 당일 최고 수익금 추적용
    last_monitor_time = 0
    last_scan_time = 0
    last_asset_record_time = 0
    last_auto_chart_time = 0
    last_sync_time = 0
    
    # 🚨 시스템 지연시간(Latency) 측정 변수 추가
    scanner_latency = 0.0
    monitor_latency = 0.0
    
    last_cleared_hour = None
    last_daily_reset_date = None
    last_snapshot_date = None 
    awaiting_setting = None
    
    is_paused = False
    current_macro_pct = 0.0 
    daily_target_notified = False  
    
    last_engine_scan_time = "스캔 대기 중"
    last_macro_state = {'KOSPI': '대기중', 'KOSDAQ': '대기중'}
    last_scanned_targets = []
    
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
        # 공통 함수: 이익 보존/목표 달성 셧다운 실행 엔진 (시장가 펀치 적용 및 계속매매 지원)
        # ---------------------------------------------------------------------
        async def execute_shutdown_sequence(reason_title, detail_msg, is_emergency=False):
            nonlocal is_paused, max_profit_today
            await send_tg_message(f"🚨 [{reason_title}]\n{detail_msg}\n지정된 보호 예외 종목을 제외하고 즉시 **시장가**로 전량 매도를 진행합니다.")
            
            sell_target = str(user_settings.get('planner_sell_target', 'auto'))
            protected = user_settings.get('protected_codes', [])
            codes_to_sell = []
            holdings_data = await kiwoom_api.get_holdings_data(session) or {}

            if sell_target == 'auto':
                codes_to_sell = [c for c in auto_watch_list.keys() if c not in protected]
            else:
                for code, h_data in holdings_data.items():
                    if code in protected: continue
                    loan_type_str = str(h_data.get('loan_type', '현금'))
                    if '신용' not in loan_type_str and '대출' not in loan_type_str:
                        codes_to_sell.append(code)

            for code in codes_to_sell:
                qty = 0
                if sell_target == 'auto': qty = auto_watch_list.get(code, {}).get('qty', 0)
                else: qty = holdings_data.get(code, {}).get('qty', 0)

                if qty > 0:
                    try:
                        res = await kiwoom_api.sell_market_order(session, code, qty)
                        await send_tg_message(f"▶️ {code} ({qty}주) 시장가 매도 전송. 결과: {res}")
                    except Exception as e:
                        await send_tg_message(f"❌ {code} 시장가 매도 실패: {e}")
            
            # 매도 후 기준 자산 및 최고 수익금 갱신 로직 추가
            await asyncio.sleep(2) # 잔고 갱신 대기
            current_assets = await kiwoom_api.get_estimated_assets(session)
            if current_assets is not None:
                user_settings['base_amount'] = int(float(str(current_assets).replace(',', '').strip()))
            
            max_profit_today = 0
            keep_trading = user_settings.get('keep_trading_after_sell', False)
            
            if keep_trading:
                is_paused = False
                await send_tg_message("♻️ [수익 실현 완료] 매도 후 기준 자산이 성공적으로 갱신되었습니다. 시스템을 중지하지 않고 새로운 매매를 계속 진행합니다.")
            else:
                is_paused = True
                await send_tg_message("🛑 [시스템 셧다운 완료] 이익 보존/목표 달성으로 모든 가동이 중지되었습니다.")

            user_settings['profit_preserve_on'] = False
            user_settings['profit_trailing_on'] = False
            def _save_set():
                with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
            await asyncio.to_thread(_save_set)

        # ---------------------------------------------------------------------
        # [비동기 멀티 태스킹 개편] 기능별 4개 태스크 분리
        # ---------------------------------------------------------------------

        # 1. 텔레그램 명령어 처리 태스크
        async def task_telegram():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date, \
                     last_macro_state, last_scanned_targets, daily_target_notified, accumulated_targets, max_profit_today, \
                     scanner_latency, monitor_latency
            
            while True:
                try:
                    now = datetime.now(KST) 
                    today_str = now.strftime('%Y%m%d')
                    new_commands = await asyncio.to_thread(telegram_bot.fetch_commands)
                    
                    for cmd in new_commands:
                        # --- [1단계] 텔레그램 콜백(버튼) 전처리 구역 ---
                        if cmd.startswith('cb:'):
                            temp_cb = cmd.replace('cb:', '')
                            # 대시보드 메뉴 라우팅 처리를 최상단에서 일반 명령어로 치환
                            if temp_cb in ['menu_setting', 'menu_monitor', 'menu_balance', 'menu_analysis']:
                                cmd = '세팅' if temp_cb == 'menu_setting' else \
                                      '감시' if temp_cb == 'menu_monitor' else \
                                      '잔고' if temp_cb == 'menu_balance' else '분석'
                            # 입력 대기 상태에서 다른 버튼을 누른 경우 꼬임 방지를 위해 강제 초기화
                            elif awaiting_setting:
                                awaiting_setting = None

                        # --- [2단계] 일반 텍스트 명령어 및 메인 실행 구역 ---
                        if cmd == 'ㄱ':
                            if len(asset_history) >= 2:
                                await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                            else:
                                await send_tg_message("⚠️ 아직 자산 차트를 그릴 충분한 데이터가 수집되지 않았습니다 (최소 20분 소요).")
                            continue

                        elif cmd == 'ㅁ':
                            msg = f"🎛️ [메인 관제탑 대시보드]\n"
                            msg += f"📡 최근 엔진 스캔: {last_engine_scan_time}\n\n"
                            msg += "원하시는 메뉴를 터치하십시오."
                            reply_markup = {
                                "inline_keyboard": [
                                    [{"text": "📈 관심종목 관리", "callback_data": "menu_watch"}, {"text": "👀 감시 현황", "callback_data": "menu_monitor"}],
                                    [{"text": "💰 계좌 잔고", "callback_data": "menu_balance"}, {"text": "📊 누적 통계 분석", "callback_data": "menu_analysis"}],
                                    [{"text": "⚙️ 엔진 세팅", "callback_data": "menu_setting"}, {"text": "💡 도움말", "callback_data": "menu_help"}],
                                    [{"text": "💸 미수/반대매 방어 (D-2)", "callback_data": "menu_margin_clear"}],
                                    [{"text": "🎯 목표 달성 플래너", "callback_data": "menu_planner"}]
                                ]
                            }
                            await send_tg_message(msg, reply_markup=reply_markup)
                            continue

                        elif cmd == '중지':
                            is_paused = True
                            await send_tg_message("🛑 [시스템 수동 중지] 모든 감시와 API 통신을 차단합니다.")
                            continue
                        
                        elif cmd == '가동':
                            is_paused = False
                            await send_tg_message("▶️ [시스템 수동 가동] 감시 및 API 통신을 정상 재개합니다.")
                            continue
                            
                        elif cmd == '분석초기화':
                            if os.path.exists('gemini_log.csv'): os.rename('gemini_log.csv', f'gemini_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                            if os.path.exists('laptop_log.csv'): os.rename('laptop_log.csv', f'laptop_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                            if os.path.exists('rvol_log.csv'): os.rename('rvol_log.csv', f'rvol_log_bak_{now.strftime("%Y%m%d_%H%M%S")}.csv')
                            await send_tg_message(f"♻️ 딥러닝 누적 데이터가 백업/초기화되었습니다.")
                            continue
                            
                        elif cmd == '분석':
                            msg = "📊 [퀀트 누적 통계 분석 리포트]\n"
                            def _build_report(file_path, engine_name, base_amt):
                                if not os.path.exists(file_path): return ""
                                try:
                                    df = pd.read_csv(file_path)
                                    if len(df) == 0: return f"\n[{engine_name}] 기록된 데이터가 없습니다.\n"
                                    def _categorize(row):
                                        reason = str(row.get('ExitReason', ''))
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
                                    
                                    rep = f"\n🔥 [{engine_name}]\n"
                                    rep += f"• 거래량: {total_cnt}건 (승 {win_cnt} / 무 {draw_cnt} / 패 {loss_cnt})\n"
                                    rep += f"• 승률: {win_rate:.1f}%\n"
                                    rep += f"• 추정 실현손익: {int(total_pnl):,}원\n"
                                    rep += f"• 평균 익절: +{avg_win:.2f}% / 평균 손절: {avg_loss:.2f}%\n"
                                    return rep
                                except Exception as e:
                                    return f"\n❌ {engine_name} 분석 오류: {e}\n"

                            msg += _build_report('gemini_log.csv', '제미나이 스캘핑', user_settings.get('gemini_amount', 500000))
                            msg += _build_report('rvol_log.csv', 'RVOLx3 스캘핑', user_settings.get('rvol_amount', 500000))
                            msg += _build_report('laptop_log.csv', '랩탑 스윙', user_settings.get('gemini_amount', 500000))
                            await send_tg_message(msg)
                            continue

                        elif cmd == '진단':
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
                                    
                                is_gem, gem_data = strategy.check_gemini_momentum_model(c3, today_str, tp_pct=user_settings.get('gemini_tp_pct', 1.5), sl_pct=user_settings.get('gemini_sl_pct', 1.0), filter_lvl=user_settings.get('gemini_filter_lvl', 2))
                                msg += f"🤖 제미나이(3m): {'✅ 포착' if is_gem else '❌ 불충족'}\n"
                                
                                is_rvol, rvol_data = strategy.check_rvol_model(c3, today_str, tp_pct=user_settings.get('gemini_tp_pct', 1.5), sl_pct=user_settings.get('gemini_sl_pct', 1.0), filter_lvl=user_settings.get('rvol_filter_lvl', 2))
                                msg += f"⚡ RVOLx3(3m): {'✅ 포착' if is_rvol else '❌ 불충족'}\n"
                                
                                is_lap, lap_data = strategy.check_laptop_swing_model(c5, today_str, user_settings['risk_amount'])
                                msg += f"💻 랩탑 스윙(5m): {'✅ 포착' if is_lap else '❌ 불충족'}\n"
                                await send_tg_message(msg)
                            continue

                        elif cmd == '잔고':
                            bal = await kiwoom_api.get_account_balance(session)
                            if bal: await send_tg_message(bal.replace('📊 ', '').replace('📋 ', ''))
                            continue

                        elif cmd == '감시취소':
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

                        elif cmd == '감시':
                            watch_msg = "👀 [현재 감시 및 스캔 현황]\n\n"
                            
                            # 🚨 실시간 지연 시간(Latency) 패널 추가
                            watch_msg += f"⏱️ [시스템 지연(Latency) 측정기]\n"
                            watch_msg += f"• 스캐너 루프: {scanner_latency:.2f}초 (총 {len(accumulated_targets)}종목 딥스캔)\n"
                            watch_msg += f"• 모니터 루프: {monitor_latency:.2f}초 (총 {len(auto_watch_list)}종목 실시간)\n\n"
                            
                            watch_msg += f"📊 시장 매크로 트렌드 (5MA-20MA)\n"
                            kpi_trend = last_macro_state.get('KOSPI', '대기중')
                            kdq_trend = last_macro_state.get('KOSDAQ', '대기중')
                            watch_msg += f"• 코스피: {'📈 정배열' if kpi_trend == '정배열' else '📉 역배열' if kpi_trend == '역배열' else kpi_trend}\n"
                            watch_msg += f"• 코스닥: {'📈 정배열' if kdq_trend == '정배열' else '📉 역배열' if kdq_trend == '역배열' else kdq_trend}\n\n"
                            
                            watch_msg += f"📡 최근 딥스캔 타겟 (Top 10)\n"
                            if last_scanned_targets:
                                watch_msg += f"• {', '.join(last_scanned_targets)}\n\n"
                            else:
                                watch_msg += "• 스캔 데이터 수집 중...\n\n"

                            watch_msg += "🎯 [현재 진입 대기 및 보유 종목]\n"
                            if not auto_watch_list: 
                                watch_msg += "• 현재 감시 중인 종목이 없습니다."
                            else:
                                for code, cond in auto_watch_list.items():
                                    status_str = "🟢 활성" if cond['status'] == 'active' else "⏳ 대기"
                                    engine_str = "RVOLx3" if cond.get('is_rvol') else ("제미나이" if cond.get('is_gemini') else "랩탑 스윙")
                                    qty_str = f"({cond.get('qty', 0)}주)"
                                    watch_msg += f"🔹 {cond.get('name', code)} {qty_str} - {engine_str} / {status_str}\n"
                            
                            reply_markup = {"inline_keyboard": [[{"text": "♻️ 잔고 강제 동기화 (수동매도 정리)", "callback_data": "sync_holdings"}]]}
                            await send_tg_message(watch_msg, reply_markup=reply_markup)
                            continue
                            
                        elif cmd == '세팅':
                            btn_gem = "🟢 제미나이 ON" if user_settings.get('engine_gem_on', True) else "🔴 제미나이 OFF"
                            btn_rvol = "🟢 RVOLx3 ON" if user_settings.get('engine_rvol_on', True) else "🔴 RVOLx3 OFF"
                            btn_lap = "🟢 랩탑 스윙 ON" if user_settings.get('engine_laptop_on', True) else "🔴 랩탑 스윙 OFF"
                            btn_time = "⏱️ 시간대 필터 ON" if user_settings.get('time_filter_on', False) else "⏱️ 시간대 필터 OFF"
                            btn_sync = "🟢 미보유 자동삭제 ON" if user_settings.get('auto_remove_unheld', False) else "🔴 미보유 자동삭제 OFF"
                            btn_gem_lvl = f"🎚️ 제미나이 필터: Lv.{user_settings.get('gemini_filter_lvl', 2)}"
                            btn_rvol_lvl = f"🎚️ RVOL 필터: Lv.{user_settings.get('rvol_filter_lvl', 2)}"
                            btn_yield = f"⬆️ 호가 양보: +{user_settings.get('buy_yield_ticks', 3)}틱"
                            btn_keep = "🟢 누적 추적 ON" if user_settings.get('keep_tracking_today', True) else "🔴 누적 추적 OFF"
                            btn_max_track = f"🎯 최대 추적: {user_settings.get('max_tracking_items', 30)}개"
                            
                            msg = f"⚙️ [시스템 세팅 현황]\n\n"
                            msg += f"🔥 초단타 공통 세팅 (제미나이 & RVOLx3)\n"
                            msg += f"• 진입 눌림 대기: -{user_settings.get('gemini_pullback_pct', 1.0)}%\n"
                            msg += f"• 자동 익절(TP): +{user_settings.get('gemini_tp_pct', 1.5)}%\n"
                            msg += f"• 자동 손절(SL): -{user_settings.get('gemini_sl_pct', 1.0)}%\n"
                            msg += f"• 미체결 취소: {user_settings.get('cancel_timeout_mins', 30)}분 경과 시\n\n"
                            
                            msg += f"🤖 제미나이 스캘핑 설정\n"
                            msg += f"• 필터 강도: Lv.{user_settings.get('gemini_filter_lvl', 2)}\n"
                            msg += f"• 고정 진입 금액: {user_settings.get('gemini_amount', 500000):,}원\n\n"
                            
                            msg += f"⚡ RVOLx3 스캘핑 설정\n"
                            msg += f"• 필터 강도: Lv.{user_settings.get('rvol_filter_lvl', 2)}\n"
                            msg += f"• 고정 진입 금액: {user_settings.get('rvol_amount', 500000):,}원\n\n"

                            msg += f"💻 랩탑 스윙 설정\n"
                            msg += f"• 1회 허용 손실(Risk): {user_settings.get('risk_amount', 500000):,}원\n"
                            msg += f"• 스윙 손익비(R:R): 1 : {user_settings.get('auto_rr_ratio', 2.0)}\n\n"
                            
                            reply_markup = {
                                "inline_keyboard": [
                                    [{"text": btn_gem, "callback_data": "toggle_gem"}, {"text": btn_rvol, "callback_data": "toggle_rvol"}, {"text": btn_lap, "callback_data": "toggle_lap"}],
                                    [{"text": btn_time, "callback_data": "toggle_time"}, {"text": btn_sync, "callback_data": "toggle_sync"}],
                                    [{"text": btn_gem_lvl, "callback_data": "cycle_gem_lvl"}, {"text": "제미나이 금액", "callback_data": "set_gemini_amount"}],
                                    [{"text": btn_rvol_lvl, "callback_data": "cycle_rvol_lvl"}, {"text": "RVOL 금액", "callback_data": "set_rvol_amount"}],
                                    [{"text": "공통 익절(TP) %", "callback_data": "set_gemini_tp"}, {"text": "공통 손절(SL) %", "callback_data": "set_gemini_sl"}],
                                    [{"text": "📉 눌림목폭(%)", "callback_data": "set_gemini_pullback"}, {"text": "수동 손실비용", "callback_data": "set_risk"}],
                                    [{"text": btn_keep, "callback_data": "toggle_keep"}, {"text": btn_max_track, "callback_data": "set_max_track"}],
                                    [{"text": "NXT 스캔 전환", "callback_data": "toggle_nxt"}, {"text": btn_yield, "callback_data": "set_buy_yield"}]
                                ]
                            }
                            await send_tg_message(msg, reply_markup=reply_markup)
                            continue

                        elif cmd.startswith('cb:'):
                            cb_data = cmd.replace('cb:', '')
                            
                            if cb_data == 'menu_help':
                                help_msg = """💡 **[시스템 도움말 가이드]**\n\n단축키 기능:\n채팅창에 아래의 글자를 입력하고 전송하세요.\n• **'ㅁ'** : 메인 관제탑 대시보드를 언제든 호출합니다.\n• **'ㄱ'** : 당일 실시간 자산 흐름 그래프를 렌더링합니다."""
                                await send_tg_message(help_msg)
                                continue
                            
                            elif cb_data == 'menu_planner':
                                try:
                                    current_assets = await kiwoom_api.get_estimated_assets(session)
                                    if current_assets is None: current_assets = user_settings.get('base_amount', 0)
                                    
                                    current_assets = int(float(str(current_assets).replace(',', '').strip()))
                                    base_amount = int(float(str(user_settings.get('base_amount', current_assets)).replace(',', '').strip()))
                                    target_date = str(user_settings.get('target_date', '2026-12-20')).strip()
                                    target_amt = int(float(str(user_settings.get('target_amount', 100000000)).replace(',', '').strip()))
                                    rem_days = int(get_remaining_trading_days(today_str, target_date))
                                    
                                    needed_amt = target_amt - current_assets
                                    today_profit = current_assets - base_amount
                                    
                                    hypo_asset = base_amount
                                    try:
                                        if os.path.exists(SNAPSHOT_FILE):
                                            with open(SNAPSHOT_FILE, 'r') as f:
                                                snap_data = json.load(f)
                                            past_dates = [d for d in snap_data.keys() if d < today_str]
                                            if past_dates:
                                                last_date = sorted(past_dates)[-1]
                                                last_holdings = snap_data[last_date]
                                                
                                                hold_pnl = 0
                                                for h_code, h_data in last_holdings.items():
                                                    qty = h_data.get('qty', 0)
                                                    if qty > 0:
                                                        rt_price = kiwoom_api.realtime_prices.get(h_code)
                                                        if not rt_price:
                                                            c1 = await kiwoom_api.get_candles(session, h_code, '1')
                                                            if c1: rt_price = abs(int(c1[0]['close']))
                                                        y_close = rt_price 
                                                        cd_d = await kiwoom_api.get_candles(session, h_code, 'D')
                                                        if cd_d and len(cd_d) > 1:
                                                            y_close = abs(int(cd_d[1]['close']))
                                                        if rt_price and y_close:
                                                            hold_pnl += (rt_price - y_close) * qty
                                                hypo_asset = base_amount + hold_pnl
                                    except Exception as e:
                                        print(f"벤치마크 연산 에러: {e}")
                                        
                                    alpha_amt = current_assets - hypo_asset
                                    rate_type = str(user_settings.get('planner_rate_type', '복리'))
                                    sell_target = str(user_settings.get('planner_sell_target', 'auto'))
                                    auto_shutdown = bool(user_settings.get('auto_shutdown_on_target', False))
                                    k_on = user_settings.get('keep_trading_after_sell', False)
                                    
                                    msg = f"🎯 [목표 달성 플래너]\n\n"
                                    msg += f"• 목표일: {target_date} (남은 영업일: {rem_days}일)\n"
                                    msg += f"• 목표 금액: {target_amt:,}원\n"
                                    msg += f"• 현재 자산: {current_assets:,}원\n"
                                    msg += f"• 기준 자산: {base_amount:,}원\n"
                                    msg += f"• 남은 금액: {needed_amt:,}원\n"
                                    msg += f"• 오늘 실제 수익금: {int(today_profit):,}원\n\n"
                                    
                                    msg += f"⚖️ **[자동매매 vs 존버(Buy&Hold) 비교]**\n"
                                    msg += f"• 어제 종목 그대로 뒀다면: {int(hypo_asset):,}원\n"
                                    msg += f"👉 **엔진 초과수익(Alpha): {f'+{int(alpha_amt):,}' if alpha_amt > 0 else f'{int(alpha_amt):,}'}원**\n\n"
                                    
                                    # 🚨 누락된 단리/복리 일일 목표 연산 로직 복원
                                    if needed_amt <= 0:
                                        msg += "🎉 **목표 금액을 이미 달성했습니다!** 축하합니다!\n\n"
                                    elif rem_days <= 0:
                                        msg += "⚠️ 목표일이 지났거나 오늘입니다. 목표일을 연장해주세요.\n\n"
                                    else:
                                        daily_req_simple = needed_amt / rem_days
                                        daily_pct_simple = (daily_req_simple / current_assets) * 100 if current_assets > 0 else 0
                                        
                                        compound_rate = 0
                                        if current_assets > 0 and target_amt > 0:
                                            compound_rate = (target_amt / current_assets) ** (1 / rem_days) - 1
                                            
                                        daily_req_compound = current_assets * compound_rate
                                        daily_pct_compound = compound_rate * 100
                                        
                                        surplus_simple = today_profit - daily_req_simple
                                        surplus_simple_str = f"+{int(surplus_simple):,}" if surplus_simple > 0 else f"{int(surplus_simple):,}"
                                        
                                        surplus_compound = today_profit - daily_req_compound
                                        surplus_compound_str = f"+{int(surplus_compound):,}" if surplus_compound > 0 else f"{int(surplus_compound):,}"
                                        
                                        msg += f"📌 **[단리 기준 (단순 평균)]**{' 👈 (현재 셧다운 기준)' if rate_type == '단리' else ''}\n"
                                        msg += f"• 오늘 목표 수익금: {int(daily_req_simple):,}원 ({daily_pct_simple:.2f}%)\n"
                                        msg += f"👉 **목표 대비 초과/미달: {surplus_simple_str}원**\n\n"

                                        msg += f"🔥 **[복리 기준 (오늘의 권장 목표)]**{' 👈 (현재 셧다운 기준)' if rate_type == '복리' else ''}\n"
                                        msg += f"• 오늘 목표 수익금: {int(daily_req_compound):,}원 ({daily_pct_compound:.2f}%)\n"
                                        msg += f"👉 **목표 대비 초과/미달: {surplus_compound_str}원**\n\n"

                                    p_on = user_settings.get('profit_preserve_on', False)
                                    p_amt = user_settings.get('profit_preserve_amount', 0)
                                    t_on = user_settings.get('profit_trailing_on', False)
                                    t_pct = user_settings.get('profit_trailing_pct', 20.0)
                                    protected = user_settings.get('protected_codes', [])

                                    msg += f"🛡️ [이익 보존 및 셧다운 보호 현황]\n"
                                    msg += f"• 고정 보존: {'🟢 ON' if p_on else '🔴 OFF'} ({p_amt:,}원 하락 시)\n"
                                    msg += f"• 추적 보존: {'🟢 ON' if t_on else '🔴 OFF'} (최고 {int(max_profit_today):,}원 대비 -{t_pct}% 시)\n"
                                    msg += f"• 보호 종목: {len(protected)}개 예외 등록됨\n"
                                    msg += f"• 목표 달성 셧다운: {'🟢 ON' if auto_shutdown else '🔴 OFF'}\n"
                                    msg += f"• 청산 후 계속매매: {'🟢 ON' if k_on else '🔴 OFF'}\n"
                                    msg += f"• 셧다운 시 매도 범위: {'🤖 시스템 종목만' if sell_target == 'auto' else '전체 현금 종목'}\n"
                                        
                                    reply_markup = {
                                        "inline_keyboard": [
                                            [{"text": "🗓️ 목표일 변경", "callback_data": "set_target_date"}, {"text": "💰 목표금액 변경", "callback_data": "set_target_amount"}],
                                            [{"text": f"🔄 셧다운 기준: {rate_type}", "callback_data": "toggle_planner_rate"}, {"text": "🔄 기준 자산 갱신", "callback_data": "set_base"}],
                                            [{"text": "🛡️ 보호종목 확인", "callback_data": "view_protected"}, {"text": f"범위: {'시스템' if sell_target=='auto' else '전체'}", "callback_data": "toggle_planner_sell"}],
                                            [{"text": f"고정보존 {'끄기' if p_on else '켜기'}", "callback_data": "toggle_profit_lock"}, {"text": "💰 보존금액 설정", "callback_data": "set_profit_amt"}],
                                            [{"text": f"추적보존 {'끄기' if t_on else '켜기'}", "callback_data": "toggle_trailing_lock"}, {"text": "📉 추적비율 설정", "callback_data": "set_trailing_pct"}],
                                            [{"text": f"목표셧다운 {'끄기' if auto_shutdown else '켜기'}", "callback_data": "toggle_planner_shutdown"}, {"text": f"계속매매 {'끄기' if k_on else '켜기'}", "callback_data": "toggle_keep_trading"}]
                                        ]
                                    }
                                    await send_tg_message(msg, reply_markup=reply_markup)
                                except Exception as e:
                                    await send_tg_message(f"❌ 플래너 연산 중 데이터 오류가 발생했습니다: {str(e)}\n설정값(기준자산 등)이 정상적인 숫자인지 확인하세요.")
                                continue

                            elif cb_data == 'toggle_planner_rate':
                                user_settings['planner_rate_type'] = '단리' if user_settings.get('planner_rate_type', '복리') == '복리' else '복리'
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                await send_tg_message(f"✅ 플래너 자동 셧다운 기준이 **{user_settings['planner_rate_type']}** 방식으로 변경되었습니다.")
                                continue
                                
                            elif cb_data == 'toggle_planner_sell':
                                user_settings['planner_sell_target'] = 'all' if user_settings.get('planner_sell_target', 'auto') == 'auto' else 'auto'
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                t_str = '전체 현금 종목' if user_settings['planner_sell_target'] == 'all' else '시스템 관리 종목'
                                await send_tg_message(f"✅ 셧다운 시 매도 범위가 **{t_str}**으로 변경되었습니다.")
                                continue
                                
                            elif cb_data == 'toggle_planner_shutdown':
                                user_settings['auto_shutdown_on_target'] = not user_settings.get('auto_shutdown_on_target', False)
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                t_str = 'ON' if user_settings['auto_shutdown_on_target'] else 'OFF'
                                await send_tg_message(f"✅ 목표 달성 셧다운 기능이 **{t_str}** 되었습니다.")
                                continue
                                
                            elif cb_data == 'toggle_keep_trading':
                                user_settings['keep_trading_after_sell'] = not user_settings.get('keep_trading_after_sell', False)
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                t_str = 'ON' if user_settings['keep_trading_after_sell'] else 'OFF'
                                await send_tg_message(f"✅ 청산(셧다운) 후 계속 매매 기능이 **{t_str}** 되었습니다.")
                                continue

                            elif cb_data == 'toggle_profit_lock':
                                user_settings['profit_preserve_on'] = not user_settings.get('profit_preserve_on', False)
                                def _save():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save)
                                await send_tg_message(f"✅ 고정 이익 보존 기능이 **{'활성화' if user_settings['profit_preserve_on'] else '비활성화'}** 되었습니다.")
                                continue

                            elif cb_data == 'toggle_trailing_lock':
                                user_settings['profit_trailing_on'] = not user_settings.get('profit_trailing_on', False)
                                def _save():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save)
                                await send_tg_message(f"✅ 비율 추적 이익 보존 기능이 **{'활성화' if user_settings['profit_trailing_on'] else '비활성화'}** 되었습니다.")
                                continue

                            elif cb_data == 'set_profit_amt':
                                awaiting_setting = 'profit_amt'
                                await send_tg_message("✏️ 보존할 최소 수익 금액(원)을 입력하세요:")
                                continue

                            elif cb_data == 'set_trailing_pct':
                                awaiting_setting = 'trailing_pct'
                                await send_tg_message("✏️ 최고 수익 대비 허용 하락 비율(%)을 입력하세요 (예: 20):")
                                continue

                            elif cb_data == 'view_protected':
                                prot = user_settings.get('protected_codes', [])
                                msg = f"🛡️ [절대 매도 보호 종목 리스트]\n\n"
                                if prot: msg += "\n".join([f"• {c}" for c in prot])
                                else: msg += "등록된 보호 종목이 없습니다."
                                await send_tg_message(msg)
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
                                current_holdings = await kiwoom_api.get_holdings_data(session) or {}
                                
                                msg = f"🚨 [미수/반대매매 방어 진단]\n"
                                msg += f"• 당일 예수금: {deposit_str}\n\n"
                                
                                if not target_holdings:
                                    msg += "❌ 해당 일자(D-2) 15시 20분의 잔고 스냅샷 기록이 존재하지 않습니다."
                                else:
                                    remaining_list = []
                                    for h_code, h_data in target_holdings.items():
                                        d2_qty = h_data.get('qty', 0)
                                        if h_code in current_holdings and current_holdings[h_code].get('qty', 0) > 0:
                                            curr_qty = current_holdings[h_code]['qty']
                                            rem_qty = min(d2_qty, curr_qty)
                                            eval_amt = current_holdings[h_code].get('purchase_price', h_data.get('purchase_price', 0)) * rem_qty
                                            remaining_list.append({
                                                'name': h_data.get('name', h_code), 
                                                'd2_qty': d2_qty,
                                                'rem_qty': rem_qty, 
                                                'eval_amt': eval_amt
                                            })
                                    
                                    if not remaining_list:
                                        msg += "✅ D-2일에 매수/보유했던 종목을 현재 모두 매도하였습니다.\n(미수 반대매매 위험 종목 없음)"
                                    else:
                                        msg += f"⚠️ [D-2일 스냅샷 기준 현재 잔존 종목]\n"
                                        remaining_list.sort(key=lambda x: x['eval_amt'], reverse=True)
                                        
                                        for i, item in enumerate(remaining_list, 1):
                                            msg += f"{i}. {item['name']}: {item['rem_qty']}주 (D-2 당시 {item['d2_qty']}주)\n"
                                            
                                        msg += "\n💡 위 종목이 미수금 발생의 원인일 수 있습니다. '매도대금 담보대출'이나 '현금 매도'로 미수를 방어하십시오."
                                        
                                await send_tg_message(msg)
                                continue

                            elif cb_data == 'set_max_track':
                                awaiting_setting = 'max_tracking_items'
                                await send_tg_message("✏️ 당일 최대 누적 추적할 종목 수를 숫자로 입력하세요 (예: 30):")
                                continue
                                
                            elif cb_data == 'set_gemini_pullback':
                                awaiting_setting = 'gemini_pullback'
                                await send_tg_message("✏️ 매수 대기 눌림폭(%)을 숫자로 입력하세요 (예: 1.0):")
                                continue
                                
                            elif cb_data == 'set_cancel_timeout':
                                awaiting_setting = 'cancel_timeout'
                                await send_tg_message("✏️ 미체결 대기주문 자동 취소 시간(분)을 숫자로 입력하세요 (예: 30):")
                                continue
                                
                            elif cb_data == 'set_buy_yield':
                                awaiting_setting = 'buy_yield_ticks'
                                await send_tg_message("✏️ 스윙 매수 시 양보할 호가 틱 수를 숫자로 입력하세요 (예: 3):")
                                continue

                            elif cb_data in ['cycle_gem_lvl', 'cycle_rvol_lvl']:
                                is_gem = (cb_data == 'cycle_gem_lvl')
                                key = 'gemini_filter_lvl' if is_gem else 'rvol_filter_lvl'
                                e_name = '제미나이' if is_gem else 'RVOLx3'
                                
                                lvl = user_settings.get(key, 2)
                                lvl = lvl + 1 if lvl < 3 else 1
                                user_settings[key] = lvl
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                
                                msg = f"✅ {e_name} 필터가 **Lv.{lvl}** 로 변경되었습니다.\n"
                                if lvl == 1: msg += "• Lv.1 (공격): 폭발 조건 + 양봉 (가장 많은 타점, VWAP 무시)"
                                elif lvl == 2: msg += "• Lv.2 (표준): Lv.1 + VWAP 0~3% 밴드 진입 허용 (추격매수 방어)"
                                elif lvl == 3: msg += "• Lv.3 (보수): Lv.2 + 60선 장기 추세 상회 (가장 엄격함)"
                                await send_tg_message(msg)
                                continue

                            toggle_map = {'toggle_gem': 'engine_gem_on', 'toggle_rvol': 'engine_rvol_on', 'toggle_lap': 'engine_laptop_on', 'toggle_time': 'time_filter_on', 'toggle_sync': 'auto_remove_unheld', 'toggle_keep': 'keep_tracking_today'}
                            if cb_data in toggle_map:
                                key = toggle_map[cb_data]
                                user_settings[key] = not user_settings.get(key, True if 'engine' in key else False)
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                status = "🟢 활성화" if user_settings[key] else "🔴 비활성화"
                                await send_tg_message(f"✅ 설정이 **{status}** 되었습니다.")
                                continue

                            elif cb_data == 'toggle_nxt':
                                user_settings['nxt_scan_enabled'] = not user_settings.get('nxt_scan_enabled', False)
                                def _save_set():
                                    with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                await asyncio.to_thread(_save_set)
                                new_status = "ON" if user_settings['nxt_scan_enabled'] else "OFF"
                                await send_tg_message(f"✅ NXT 연장장 스캔 모드가 **{new_status}** 되었습니다.")
                                continue
                            
                            elif cb_data in ['set_base', 'set_risk', 'set_autorr', 'set_gemini_amount', 'set_rvol_amount', 'set_gemini_tp', 'set_gemini_sl', 'set_target_date', 'set_target_amount']:
                                awaiting_setting = cb_data.replace('set_', '')
                                if awaiting_setting == 'target_date':
                                    await send_tg_message("🗓️ 새로운 목표일을 YYYY-MM-DD 형식으로 입력하세요 (예: 2026-12-20):")
                                else:
                                    await send_tg_message("✏️ 새로운 값을 숫자로만 입력하세요.")
                                continue

                        elif awaiting_setting:
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

                            elif awaiting_setting == 'profit_amt':
                                try:
                                    val = int(cmd.replace(',', '').strip())
                                    cur_assets = await kiwoom_api.get_estimated_assets(session)
                                    cur_profit = int(float(str(cur_assets).replace(',', ''))) - user_settings.get('base_amount', 0)
                                    if val >= cur_profit and cur_profit > 0:
                                        await send_tg_message(f"❌ 보존 금액({val:,}원)은 현재 당일 수익({cur_profit:,}원)보다 낮아야 합니다.")
                                    else:
                                        user_settings['profit_preserve_amount'] = val
                                        user_settings['profit_preserve_on'] = True
                                        def _save():
                                            with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                        await asyncio.to_thread(_save)
                                        await send_tg_message(f"✅ 고정 이익 보존선이 {val:,}원으로 설정 및 활성화되었습니다.")
                                except: await send_tg_message("❌ 올바른 숫자를 입력하세요.")
                                awaiting_setting = None
                                continue

                            elif awaiting_setting == 'trailing_pct':
                                try:
                                    val = float(cmd.strip())
                                    user_settings['profit_trailing_pct'] = val
                                    user_settings['profit_trailing_on'] = True
                                    def _save():
                                        with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                    await asyncio.to_thread(_save)
                                    await send_tg_message(f"✅ 비율 추적 하락 폭이 {val}%로 설정 및 활성화되었습니다.")
                                except: await send_tg_message("❌ 숫자를 입력하세요.")
                                awaiting_setting = None
                                continue

                            else:
                                try:
                                    val = float(cmd.replace(',', '').strip())
                                    if awaiting_setting in ['base', 'risk']: user_settings[f"{awaiting_setting}_amount"] = int(val)
                                    elif awaiting_setting == 'buy_yield_ticks': user_settings['buy_yield_ticks'] = int(val)
                                    elif awaiting_setting == 'gemini_amount': user_settings['gemini_amount'] = int(val)
                                    elif awaiting_setting == 'rvol_amount': user_settings['rvol_amount'] = int(val)
                                    elif awaiting_setting == 'gemini_tp': user_settings['gemini_tp_pct'] = val
                                    elif awaiting_setting == 'gemini_sl': user_settings['gemini_sl_pct'] = val
                                    elif awaiting_setting == 'gemini_pullback': user_settings['gemini_pullback_pct'] = val
                                    elif awaiting_setting == 'cancel_timeout': user_settings['cancel_timeout_mins'] = int(val)
                                    elif awaiting_setting == 'autorr': user_settings['auto_rr_ratio'] = val
                                    elif awaiting_setting == 'target_amount': user_settings['target_amount'] = int(val)
                                    elif awaiting_setting == 'max_tracking_items': user_settings['max_tracking_items'] = int(val)
                                        
                                    await send_tg_message("✅ 설정 변경이 완료되었습니다.")
                                    def _save_set():
                                        with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                                    await asyncio.to_thread(_save_set)
                                except ValueError: await send_tg_message("❌ 숫자만 입력해야 합니다.")
                                finally: awaiting_setting = None
                                continue

                except Exception as e:
                    traceback.print_exc()
                    print(f"🚨 [System] task_telegram 크래시 발생 및 복구 중: {e}")
                
                await asyncio.sleep(0.5)

        # 2. 주기적 스케줄링 태스크
        async def task_scheduler():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date, \
                     daily_target_notified, accumulated_targets, max_profit_today
            
            while True:
                try:
                    current_timestamp = time.time()
                    now = datetime.now(KST) 
                    today_str = now.strftime('%Y%m%d')
                    is_weekend = (now.weekday() >= 5)
                    is_active_day = not is_weekend and not is_paused
                    is_trade_time = dt_time(9, 0) <= now.time() <= (dt_time(20, 0) if user_settings.get('nxt_scan_enabled', False) else dt_time(15, 30))

                    # 매일 아침 초기화
                    if now.hour == 7 and now.minute == 55 and last_daily_reset_date != today_str:
                        if len(asset_history) >= 2:
                            prev_date_str = asset_history[-1][0].strftime('%Y%m%d')
                            archive_path = f"asset_chart_{prev_date_str}.png"
                            if os.path.exists('asset_chart.png'):
                                await asyncio.to_thread(shutil.copy, 'asset_chart.png', archive_path)
                                
                        current_assets = await kiwoom_api.get_estimated_assets(session)
                        if current_assets is not None:
                            user_settings['base_amount'] = int(float(str(current_assets).replace(',', '').strip()))
                            def _save_set():
                                with open(SETTINGS_FILE, 'w') as f: json.dump(user_settings, f, indent=4)
                            await asyncio.to_thread(_save_set)

                        asset_history.clear()    
                        max_assets_today = 0
                        max_assets_time = ""
                        max_profit_today = 0      # 🚨 당일 최고 수익 초기화
                        is_paused = False 
                        daily_target_notified = False
                        alerted_obs.clear()
                        pending_setups.clear()
                        accumulated_targets.clear() # 🚨 일일 누적 추적 목록 초기화
                        await save_alerted_obs(alerted_obs)
                        last_daily_reset_date = today_str
                        last_engine_scan_time = "스캔 대기 중"
                        
                        msg = f"🌅 [07:55] 시스템 초기화 및 전일자 차트 보관 완료.\n"
                        msg += f"💰 금일 시작 기준 자산: {user_settings.get('base_amount', 0):,}원"
                        await send_tg_message(msg)
                    
                    # 매 정각 화면 지우기
                    if now.minute == 0 and last_cleared_hour != now.hour:
                        os.system('cls' if os.name == 'nt' else 'clear')
                        last_cleared_hour = now.hour

                    # 1분 단위 잔고 상태 동기화 (수동 매도 감지)
                    if is_active_day and is_trade_time and (current_timestamp - last_sync_time > 60):
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
                    if is_active_day and now.second >= 3 and (current_timestamp - last_asset_record_time > 50):
                        is_premarket = (now.hour == 8 and 50 <= now.minute <= 59)
                        is_open_lag = (now.hour == 9 and now.minute == 0)
                        if not is_premarket and not is_open_lag:
                            current_assets = await kiwoom_api.get_estimated_assets(session)
                            if current_assets is not None:
                                current_assets = int(float(str(current_assets).replace(',', '').strip()))
                                
                                if current_assets > max_assets_today:
                                    max_assets_today, max_assets_time = current_assets, now.strftime('%H:%M:%S')
                                
                                is_notify_time = (dt_time(8, 0) <= now.time() <= dt_time(20, 0))
                                if is_notify_time:
                                    asset_history.append((now, current_assets))
                                    await asyncio.to_thread(lambda: csv.writer(open(ASSET_FILE, 'a', newline='')).writerow([now.strftime('%Y-%m-%d %H:%M:%S'), current_assets]))
                                
                                try:
                                    base_amount = int(float(str(user_settings.get('base_amount', current_assets)).replace(',', '').strip()))
                                    target_amt = int(float(str(user_settings.get('target_amount', 100000000)).replace(',', '').strip()))
                                    target_date = str(user_settings.get('target_date', '2026-12-20')).strip()
                                    rem_days = int(get_remaining_trading_days(today_str, target_date))
                                    
                                    today_profit = current_assets - base_amount
                                    
                                    # 🚨 당일 최고 수익금 실시간 갱신
                                    if today_profit > max_profit_today: max_profit_today = today_profit
                                    
                                    # 🚨 로직 1: 고정 금액 기준 이익 보존 검사
                                    if user_settings.get('profit_preserve_on') and today_profit > 0 and not is_paused:
                                        p_amt = user_settings.get('profit_preserve_amount', 0)
                                        if today_profit <= p_amt and p_amt > 0:
                                            await execute_shutdown_sequence("🛡️ 고정 이익 보존 발동", f"수익({int(today_profit):,}원)이 설정된 보존선({p_amt:,}원) 이하로 하락했습니다.")

                                    # 🚨 로직 2: 비율형 추적(Trailing) 이익 보존 검사
                                    if user_settings.get('profit_trailing_on') and max_profit_today > 0 and not is_paused:
                                        t_pct = user_settings.get('profit_trailing_pct', 20.0)
                                        threshold = max_profit_today * (1 - t_pct / 100.0)
                                        if today_profit <= threshold and today_profit > 0:
                                            await execute_shutdown_sequence("📉 추적 비율 이익 보존 발동", f"현재 수익({int(today_profit):,}원)이 당일 최고 수익({int(max_profit_today):,}원) 대비 {t_pct}% 이상 하락했습니다.")

                                    if rem_days > 0 and current_assets > base_amount:
                                        rate_type = str(user_settings.get('planner_rate_type', '복리'))
                                        if rate_type == '복리':
                                            compound_rate = 0
                                            if current_assets > 0 and target_amt > 0:
                                                compound_rate = (target_amt / current_assets) ** (1 / rem_days) - 1
                                            daily_req = current_assets * compound_rate
                                        else:
                                            daily_req = (target_amt - current_assets) / rem_days
                                            
                                        if daily_req > 0 and today_profit >= daily_req and not daily_target_notified:
                                            await send_tg_message(f"🎊 [일일 목표 달성!]\n오늘의 {rate_type} 목표 수익({int(daily_req):,}원)을 성공적으로 돌파했습니다! (현재: +{int(today_profit):,}원)\n(수익 2배 도달 시 설정에 따라 자동 셧다운이 발동될 수 있습니다.)")
                                            daily_target_notified = True

                                        target_threshold = daily_req * 2
                                        if user_settings.get('auto_shutdown_on_target', False) and not is_paused:
                                            if today_profit >= target_threshold and target_threshold > 0:
                                                await execute_shutdown_sequence("🎉 목표 달성 자동 매도 발동", f"오늘 수익({int(today_profit):,}원)이 일일 목표의 2배({int(target_threshold):,}원)를 돌파했습니다!")
                                                
                                except Exception as e:
                                    print(f"🚨 [스케줄러 오류]: 셧다운/목표알림 검사 중 예외 발생 - {e}")

                        last_asset_record_time = time.time()

                    # 자산 차트 10분마다 자동 발송
                    is_notify_time = (dt_time(8, 0) <= now.time() <= dt_time(20, 0))
                    if is_active_day and is_notify_time and now.minute % 10 == 0 and now.second >= 10 and (current_timestamp - last_auto_chart_time > 60):
                        if len(asset_history) >= 2:
                            await asyncio.to_thread(_generate_and_send_asset_chart, asset_history, user_settings['base_amount'], max_assets_today, max_assets_time)
                        last_auto_chart_time = current_timestamp
                        
                except Exception as e:
                    traceback.print_exc()
                    print(f"🚨 [System] task_scheduler 크래시 발생 및 복구 중: {e}")
                    
                await asyncio.sleep(1)

        # 3. 시장 조건 검색 스캐너 태스크
        async def task_scanner():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date, \
                     last_macro_state, last_scanned_targets, accumulated_targets, scanner_latency
            
            while True:
                try:
                    current_timestamp = time.time()
                    now = datetime.now(KST) 
                    today_str = now.strftime('%Y%m%d')
                    is_weekend = (now.weekday() >= 5)
                    is_active_day = not is_weekend and not is_paused

                    scan_end_time = dt_time(20, 0) if user_settings.get('nxt_scan_enabled', False) else dt_time(15, 30)
                    is_scan_time = (dt_time(9, 0) <= now.time() <= scan_end_time) and is_active_day

                    allow_gem = user_settings.get('engine_gem_on', True)
                    allow_rvol = user_settings.get('engine_rvol_on', True)
                    allow_lap = user_settings.get('engine_laptop_on', True)
                    
                    if user_settings.get('time_filter_on', False):
                        if not (9 <= now.hour <= 10 and (now.hour == 9 or now.minute <= 30)):
                            allow_gem, allow_rvol, allow_lap = False, False, False

                    # 시장 서킷 브레이커 방어 (과거 등락률 로직 유지)
                    if current_macro_pct <= -1.5:
                        allow_gem, allow_rvol, allow_lap = False, False, False

                    if is_scan_time and (current_timestamp - last_scan_time > 50):
                        _scan_start_time = time.time()  # 🚨 스캐너 실행 시간 측정 시작
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
                            
                            # 타겟 후보 수집 및 🚨 FIFO 로직 처리
                            search_20 = await kiwoom_api.get_top_20_search_rank(session)
                            volume_20 = await kiwoom_api.get_top_20_volume_rank(session)
                            
                            new_candidates = {}
                            if search_20: new_candidates.update(search_20)
                            if volume_20: new_candidates.update(volume_20)
                            
                            if user_settings.get('keep_tracking_today', True):
                                for code, name in new_candidates.items():
                                    if code not in accumulated_targets:
                                        accumulated_targets[code] = name
                                        
                                max_track = user_settings.get('max_tracking_items', 30)
                                while len(accumulated_targets) > max_track:
                                    oldest = next(iter(accumulated_targets))
                                    del accumulated_targets[oldest]
                                    
                                target_candidates = accumulated_targets.copy()
                            else:
                                target_candidates = new_candidates.copy()
                            
                            target_codes = list(target_candidates.keys())
                            stock_dict.update(target_candidates)
                            
                            watchlist = user_settings.get('custom_watchlist', {})
                            for wc_code, wc_name in watchlist.items():
                                if wc_code not in target_codes:
                                    target_codes.append(wc_code)
                                    stock_dict[wc_code] = wc_name
                                    
                            last_macro_state['KOSPI'] = macro_state['KOSPI']['trend']
                            last_macro_state['KOSDAQ'] = macro_state['KOSDAQ']['trend']
                            last_scanned_targets = [stock_dict.get(c, c) for c in target_codes[:10]]
                                    
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
                                    
                                    # 정배열 킬 스위치 (역배열 시 매수 차단)
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

                                        # ⚡ RVOLx3 스캘핑 엔진 스캔
                                        if allow_rvol:
                                            is_rvol, rvol_data = strategy.check_rvol_model(
                                                candles_3m, today_str, 
                                                tp_pct=user_settings.get('gemini_tp_pct', 1.5), 
                                                sl_pct=user_settings.get('gemini_sl_pct', 1.0), 
                                                filter_lvl=user_settings.get('rvol_filter_lvl', 2)
                                            )
                                            if is_rvol:
                                                alert_key = f"{code}_3m_RVOL_{rvol_data['time']}"
                                                if alert_key not in alerted_obs:
                                                    alerted_obs.add(alert_key)
                                                    await save_alerted_obs(alerted_obs)
                                                    entry = rvol_data['entry_price']
                                                    pullback_pct = user_settings.get('gemini_pullback_pct', 1.0)
                                                    target_price_raw = int(entry * (1 - pullback_pct / 100.0))
                                                    tick_size = _get_tick_size(target_price_raw)
                                                    order_price = (target_price_raw // tick_size) * tick_size
                                                    
                                                    qty = user_settings.get('rvol_amount', 500000) // order_price
                                                    if qty > 0:
                                                        buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, order_price)
                                                        if "✅" in buy_res:
                                                            rvol_data['meta']['macro_pct'] = current_macro_pct 
                                                            rvol_data['meta']['macro_trend'] = m_state.get('trend', '정배열')
                                                            rvol_data['meta']['macro_gap'] = m_state.get('gap', 0.0)
                                                            auto_watch_list[code] = {'name': stock_dict[code], 'market': market, 'qty': qty, 'entry': order_price, 'sl': rvol_data['sl_price'], 'tp': rvol_data['dynamic_tp'], 'status': 'pending', 'odno': odno, 'is_rvol': True, 'meta': rvol_data['meta'], 'entry_time': time.time(), 'original_entry': entry}
                                                            await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                            if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                            msg = f"⚡ [RVOLx3 폭발 대기매수] {stock_dict[code]} {qty}주\n"
                                                            msg += f"• 포착가: {entry:,}원 ➡️ 대기 매수가: {order_price:,}원 (-{pullback_pct}%)\n"
                                                            msg += f"• 체결 시 목표가: {rvol_data['dynamic_tp']:,}원 (+{user_settings.get('gemini_tp_pct', 1.5)}%)\n"
                                                            msg += f"• 체결 시 손절가: {rvol_data['sl_price']:,}원 (-{user_settings.get('gemini_sl_pct', 1.0)}%)\n"
                                                            msg += f"• 진단: {rvol_data['meta'].get('diag_msg', '확인불가')}"
                                                            await send_tg_message(msg)
                                                    continue # RVOL 포착 시 제미나이 검사 스킵

                                        # 🤖 제미나이 스캘핑 엔진 스캔
                                        if allow_gem:
                                            is_gemini, gemini_data = strategy.check_gemini_momentum_model(
                                                candles_3m, today_str, 
                                                tp_pct=user_settings.get('gemini_tp_pct', 1.5), 
                                                sl_pct=user_settings.get('gemini_sl_pct', 1.0), 
                                                filter_lvl=user_settings.get('gemini_filter_lvl', 2)
                                            )
                                            if is_gemini:
                                                alert_key = f"{code}_3m_GEMINI_{gemini_data['time']}"
                                                if alert_key not in alerted_obs:
                                                    alerted_obs.add(alert_key)
                                                    await save_alerted_obs(alerted_obs)
                                                    entry = gemini_data['entry_price']
                                                    pullback_pct = user_settings.get('gemini_pullback_pct', 1.0)
                                                    target_price_raw = int(entry * (1 - pullback_pct / 100.0))
                                                    tick_size = _get_tick_size(target_price_raw)
                                                    order_price = (target_price_raw // tick_size) * tick_size
                                                    
                                                    qty = user_settings.get('gemini_amount', 500000) // order_price
                                                    if qty > 0:
                                                        buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, order_price)
                                                        if "✅" in buy_res:
                                                            gemini_data['meta']['macro_pct'] = current_macro_pct 
                                                            gemini_data['meta']['macro_trend'] = m_state.get('trend', '정배열')
                                                            gemini_data['meta']['macro_gap'] = m_state.get('gap', 0.0)
                                                            auto_watch_list[code] = {'name': stock_dict[code], 'market': market, 'qty': qty, 'entry': order_price, 'sl': gemini_data['sl_price'], 'tp': gemini_data['dynamic_tp'], 'status': 'pending', 'odno': odno, 'is_gemini': True, 'meta': gemini_data['meta'], 'entry_time': time.time(), 'original_entry': entry}
                                                            await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                            if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                            msg = f"🤖 [제미나이 눌림목 대기매수] {stock_dict[code]} {qty}주\n"
                                                            msg += f"• 포착가: {entry:,}원 ➡️ 대기 매수가: {order_price:,}원 (-{pullback_pct}%)\n"
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
                                                    yield_ticks = user_settings.get('buy_yield_ticks', 3)
                                                    tick_size = _get_tick_size(entry)
                                                    order_price = entry + (tick_size * yield_ticks)
                                                    
                                                    qty = lap_data['qty'] 
                                                    if qty > 0:
                                                        buy_res, odno = await kiwoom_api.buy_limit_order(session, code, qty, order_price)
                                                        if "✅" in buy_res:
                                                            lap_data['meta']['macro_pct'] = current_macro_pct 
                                                            lap_data['meta']['macro_trend'] = m_state.get('trend', '정배열')
                                                            lap_data['meta']['macro_gap'] = m_state.get('gap', 0.0)
                                                            auto_watch_list[code] = {'name': stock_dict[code], 'market': market, 'qty': qty, 'entry': order_price, 'sl': lap_data['sl_price'], 'tp': lap_data['dynamic_tp'], 'status': 'pending', 'odno': odno, 'is_laptop': True, 'meta': lap_data['meta'], 'entry_time': time.time(), 'original_entry': entry}
                                                            await save_watch_list(redis_client, auto_watch_list, use_redis)
                                                            if kiwoom_api.ws_client: await kiwoom_api.ws_client.subscribe(code)
                                                            await send_tg_message(f"💻 [랩탑 스윙 대기매수] {stock_dict[code]} {qty}주 (매수가: {order_price:,}원, +{yield_ticks}호가 양보)")
                                                continue
                        scanner_latency = round(time.time() - _scan_start_time, 2)  # 🚨 스캐너 실행 완료 후 지연시간 기록
                        last_scan_time = time.time()
                except Exception as e:
                    traceback.print_exc()
                    print(f"🚨 [System] task_scanner 크래시 발생 및 복구 중: {e}")
                    
                await asyncio.sleep(1)

        # 4. 실시간 웹소켓 가격 감시 태스크 (블로킹 우회)
        async def task_monitor():
            nonlocal is_paused, current_macro_pct, last_engine_scan_time, max_assets_today, max_assets_time, \
                     awaiting_setting, last_monitor_time, last_scan_time, last_asset_record_time, \
                     last_auto_chart_time, last_sync_time, last_cleared_hour, last_daily_reset_date, last_snapshot_date, \
                     monitor_latency
            
            while True:
                try:
                    current_timestamp = time.time()
                    now = datetime.now(KST)
                    is_weekend = (now.weekday() >= 5)
                    is_active_day = not is_weekend and not is_paused
                    is_monitor_time = dt_time(9, 0) <= now.time() <= (dt_time(20, 0) if user_settings.get('nxt_scan_enabled', False) else dt_time(15, 30))

                    if is_active_day and is_monitor_time and auto_watch_list and (current_timestamp - last_monitor_time >= 1):
                        _mon_start_time = time.time()  # 🚨 모니터 실행 시간 측정 시작
                        state_changed = False
                        
                        for code, cond in list(auto_watch_list.items()):
                            rt_price = kiwoom_api.realtime_prices.get(code)
                            if not rt_price:
                                async with api_semaphore:
                                    c1 = await kiwoom_api.get_candles(session, code, '1')
                                if not c1: continue
                                rt_price = abs(int(c1[0]['close']))
                                
                            rt_price = int(float(str(rt_price).replace(',', '').strip()))
                            stock_name = cond.get('name', code)
                            
                            if cond['status'] == 'pending':
                                timeout_mins = user_settings.get('cancel_timeout_mins', 30)
                                if (current_timestamp - cond.get('entry_time', current_timestamp)) > timeout_mins * 60:
                                    if cond.get('odno'):
                                        await kiwoom_api.cancel_order(session, code, cond['odno'])
                                    del auto_watch_list[code]
                                    if kiwoom_api.ws_client: await kiwoom_api.ws_client.unsubscribe(code)
                                    state_changed = True
                                    await send_tg_message(f"⏳ [{stock_name}] 미체결 대기시간({timeout_mins}분) 초과로 매수 주문 자동 취소 완료.")
                                    continue

                                # 🚨 상태가 pending일 때만 매수 체결 검사 수행
                                if rt_price <= cond['entry']:
                                    cond['status'] = 'active'
                                    cond['max_reached'] = rt_price
                                    cond['min_reached'] = rt_price 
                                    state_changed = True
                                    await send_tg_message(f"🟢 [{stock_name}] 매수 체결 확인! 감시 활성화.")
                            
                            if cond['status'] == 'active':
                                if 'max_reached' not in cond: cond['max_reached'] = rt_price
                                if 'min_reached' not in cond: cond['min_reached'] = rt_price
                                
                                if rt_price > cond['max_reached']: cond['max_reached'] = rt_price
                                if rt_price < cond['min_reached']: cond['min_reached'] = rt_price
                                    
                                if rt_price >= cond['entry'] * 1.01 and cond['sl'] < cond['entry']:
                                    cond['sl'] = cond['entry']
                                    state_changed = True
                                    await send_tg_message(f"🛡️ [{stock_name}] +1.0% 수익 도달! 손절가를 매수가({cond['entry']:,}원)로 상향 조정하여 원금을 방어합니다.")

                                if cond['tp'] > 0 and rt_price >= cond['tp']:
                                    sell_qty = max(1, cond.get('qty', 1) // 2)
                                    sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                                    
                                    if "✅" in sell_res:
                                        await send_tg_message(f"🚀 [{stock_name}] 1차 목표가({cond['tp']:,}원) 도달! 50% 분할 익절 청산(시장가).\n결과: {sell_res}")
                                        pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                        max_pnl = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                        min_pnl = round(((cond['min_reached'] - cond['entry']) / cond['entry']) * 100, 2) 
                                        slippage = round(((rt_price - cond['tp']) / cond['tp']) * 100, 2) if cond['tp'] > 0 else 0.0 
                                        session_str = get_time_session(cond.get('entry_time', time.time())) 
                                        
                                        hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                        meta = cond.get('meta', {})
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'VolBurstRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, "50%분할익절"]
                                            
                                        if cond.get('is_rvol'):
                                            await asyncio.to_thread(write_trade_log, 'rvol_log.csv', headers, row)
                                        elif cond.get('is_gemini'):
                                            await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                        elif cond.get('is_laptop'):
                                            headers[11] = 'PullbackVolRatio'
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

                                if cond.get('half_sold'):
                                    new_sl_raw = int(cond['max_reached'] * 0.98)
                                    tick = _get_tick_size(new_sl_raw)
                                    new_sl = (new_sl_raw // tick) * tick if tick > 1 else new_sl_raw
                                        
                                    if new_sl > cond['sl']:
                                        cond['sl'] = new_sl
                                        state_changed = True

                                if code in auto_watch_list and cond['sl'] > 0 and rt_price <= cond['sl']:
                                    sell_qty = cond.get('qty', 1)
                                    sell_res = await kiwoom_api.sell_market_order(session, code, sell_qty)
                                    
                                    if "✅" in sell_res:
                                        reason = "트레일링스탑(익절)" if cond.get('half_sold') else ("본절방어" if cond['sl'] == cond['entry'] else "손절(SL)")
                                        icon = "💸" if reason == "트레일링스탑(익절)" else ("🛡️" if reason == "본절방어" else "🔴")
                                        await send_tg_message(f"{icon} [{stock_name}] 손절/익절선({cond['sl']:,}원) 이탈! 남은 수량 전량 청산(시장가).\n사유: {reason}\n결과: {sell_res}")
                                        
                                        pnl_pct = round(((rt_price - cond['entry']) / cond['entry']) * 100, 2)
                                        max_pnl = round(((cond['max_reached'] - cond['entry']) / cond['entry']) * 100, 2)
                                        min_pnl = round(((cond['min_reached'] - cond['entry']) / cond['entry']) * 100, 2) 
                                        slippage = round(((rt_price - cond['sl']) / cond['sl']) * 100, 2) if cond['sl'] > 0 else 0.0 
                                        session_str = get_time_session(cond.get('entry_time', time.time())) 
                                        
                                        hold_sec = int(time.time() - cond.get('entry_time', time.time()))
                                        meta = cond.get('meta', {})
                                        headers = ['Date', 'Code', 'Name', 'Market', 'TimeSession', 'EntryPrice', 'ExitPrice', 'Slippage(%)', 'PnL(%)', 'MinPnL(%)', 'MaxPnL(%)', 'VolBurstRatio', 'EntryATR', 'EntryVWAPGap(%)', 'MacroTrend', 'MacroGap(%)', 'HoldTime(s)', 'ExitReason']
                                        row = [datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'), code, cond['name'], cond.get('market', 'KOSDAQ'), session_str, cond['entry'], rt_price, slippage, pnl_pct, min_pnl, max_pnl, meta.get('vol_burst_ratio', 0), meta.get('entry_atr', 0), meta.get('vwap_gap', 0.0), meta.get('macro_trend', '정배열'), meta.get('macro_gap', 0.0), hold_sec, reason]
                                            
                                        if cond.get('is_rvol'):
                                            await asyncio.to_thread(write_trade_log, 'rvol_log.csv', headers, row)
                                        elif cond.get('is_gemini'):
                                            await asyncio.to_thread(write_trade_log, 'gemini_log.csv', headers, row)
                                        elif cond.get('is_laptop'):
                                            headers[11] = 'PullbackVolRatio'
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
                        monitor_latency = round(time.time() - _mon_start_time, 2)  # 🚨 모니터 실행 완료 후 지연시간 기록
                        last_monitor_time = time.time()
                except Exception as e:
                    traceback.print_exc()
                    print(f"🚨 [System] task_monitor 크래시 발생 및 복구 중: {e}")
                        
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