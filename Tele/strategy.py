import math
import json
import logging
import kiwoom_api  # 미체결 및 잔고 조회를 위한 모듈

logger = logging.getLogger(__name__)

# 상태 관리를 위한 파일명 정의
WATCH_LIST_FILE = "watch_list_state.json"

# -----------------------------------------------------------------------------
# 1. 공통 퀀트 수학 및 호가창 유틸리티 모듈
# -----------------------------------------------------------------------------
def get_tick_size(price):
    if price < 2000: return 1
    if price < 5000: return 5
    if price < 20000: return 10
    if price < 50000: return 50
    if price < 200000: return 100
    if price < 500000: return 500
    return 1000

def count_exact_ticks(p1, p2):
    if p1 == p2: return 0
    low_p, high_p = min(p1, p2), max(p1, p2)
    ticks = 0
    curr = low_p
    while curr < high_p:
        curr += get_tick_size(curr)
        ticks += 1
    return ticks

def floor_to_tick(price):
    tick = get_tick_size(price)
    return (int(price) // tick) * tick

def ceil_to_tick(price):
    tick = get_tick_size(price)
    return math.ceil(price / tick) * tick

def validate_stale_data(candles, today_str):
    if not candles: return False
    return candles[0]['time'].startswith(today_str)

def calculate_vwap(candles, today_str):
    cum_vol = 0
    cum_pv = 0
    for c in reversed(candles):
        if c['time'].startswith(today_str):
            typ_price = (c['high'] + c['low'] + c['close']) / 3
            cum_vol += c['volume']
            cum_pv += typ_price * c['volume']
    return cum_pv / cum_vol if cum_vol > 0 else 0

def calculate_atr(candles, period=14):
    if len(candles) < period + 1: return 0
    trs = []
    for i in range(period):
        curr = candles[i]
        prev = candles[i+1]
        tr = max(curr['high'] - curr['low'], abs(curr['high'] - prev['close']), abs(curr['low'] - prev['close']))
        trs.append(tr)
    return sum(trs) / len(trs)

def check_long_trend(candles, period=60):
    if len(candles) < period: return True
    closes = [c['close'] for c in candles[:period]]
    sma = sum(closes) / len(closes)
    return candles[0]['close'] > sma

def get_ema(closes_old_to_new, period):
    if len(closes_old_to_new) < period:
        return 0
    k = 2 / (period + 1)
    ema = sum(closes_old_to_new[:period]) / period
    for price in closes_old_to_new[period:]:
        ema = (price * k) + (ema * (1 - k))
    return ema

# -----------------------------------------------------------------------------
# 2. 제미나이 모멘텀 스캘핑 엔진 (눌림목 대기 매수 로직 적용)
# -----------------------------------------------------------------------------
def check_gemini_momentum_model(candles, today_str, tp_pct=1.5, sl_pct=1.0, filter_lvl=3):
    if not candles or len(candles) < 60: return False, {}
    n1 = candles[1]
    
    # [Lv.1 공통 조건] 양봉 및 거래량 2배 폭발 확인
    if n1['close'] <= n1['open']: return False, {}
        
    recent_vols = [c['volume'] for c in candles[2:16]]
    avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
    if n1['volume'] < (avg_vol * 2.0): return False, {}

    # 상위 필터 조건 검사
    vwap = calculate_vwap(candles, today_str)
    is_above_vwap = (n1['close'] >= vwap)
    is_long_trend = check_long_trend(candles, 60)

    if filter_lvl >= 2 and not is_above_vwap: return False, {}
    if filter_lvl >= 3 and not is_long_trend: return False, {}
    
    fail_reasons = []
    if not is_above_vwap: fail_reasons.append("VWAP 하회(Lv.2 미달)")
    if not is_long_trend: fail_reasons.append("60선 역배열(Lv.3 미달)")
    diag_msg = " + ".join(fail_reasons) if fail_reasons else "모든 조건 충족(Lv.3급 완벽)"

    # 🚨 [수정] 현재가가 아닌 ATR 기반 손절가 우선 계산
    current_price = n1['close']
    atr = calculate_atr(candles, 14)
    if atr == 0: return False, {}
    
    # 기본 ATR 1.5배수 손절선 계산 (호가 내림)
    base_sl_price = floor_to_tick(current_price - (atr * 1.5))
    
    # 🚨 [수정] 계산된 손절선 위 1.0%(sl_pct) 지점을 '지정가 대기 매수가'로 역산
    # 매수가 = 손절가 / (1 - 손절비율)
    calc_entry_price = floor_to_tick(base_sl_price / (1 - (sl_pct / 100.0)))
    
    # 대기 매수가가 현재가(종가)보다 높거나 같으면 추격매수이므로 포기
    if calc_entry_price >= current_price: return False, {}
    
    # 🚨 [수정] 최종 익절가(TP)는 '대기 매수가' 기준 +1.5%(tp_pct) 계산
    tp_price = ceil_to_tick(calc_entry_price * (1 + (tp_pct / 100.0)))
    
    return True, {
        'time': n1['time'],
        'entry_price': calc_entry_price, # 눌림목 대기 매수가
        'sl_price': base_sl_price,       # 최종 손절가
        'dynamic_tp': tp_price,          # 최종 목표가
        'strategy': 'GEMINI',
        'meta': {
            'vol_burst_ratio': round(n1['volume'] / avg_vol, 2),
            'entry_atr': round(atr, 2),
            'upper_tail_ratio': 0,
            'strategy': 'GEMINI',
            'diag_msg': diag_msg
        }
    }

# -----------------------------------------------------------------------------
# 3. 랩탑 스윙 엔진 (VWAP + 9/20 EMA 추세추종 - 눌림목 대기 매수 로직 적용)
# -----------------------------------------------------------------------------
def check_laptop_swing_model(candles, today_str, risk_amount, rr_ratio=2.0):
    if not candles or len(candles) < 40: return False, {}
    
    n1 = candles[1] 
    n2 = candles[2] 
    
    if not check_long_trend(candles, 40): return False, {}

    vwap = calculate_vwap(candles, today_str)
    if n1['close'] < vwap or n2['close'] < vwap: return False, {}
    
    closes_old_to_new = [c['close'] for c in reversed(candles)]
    
    ema_9 = get_ema(closes_old_to_new[:-1], 9) 
    ema_20 = get_ema(closes_old_to_new[:-1], 20)
    
    if ema_9 <= ema_20: return False, {}
    if n2['close'] > ema_9 or n1['close'] <= ema_9: return False, {}
    
    recent_10_vols = [c['volume'] for c in candles[3:13]]
    avg_vol = sum(recent_10_vols) / len(recent_10_vols) if recent_10_vols else 1
    impulse_exists = any(v > avg_vol * 2.5 for v in recent_10_vols)
    if not impulse_exists: return False, {}
    
    if n2['volume'] > (max(recent_10_vols) * 0.7): return False, {}

    # 🚨 [수정] 랩탑 엔진도 ATR 기반 손절선 먼저 계산
    current_price = n1['close']
    atr = calculate_atr(candles, 14)
    if atr == 0: return False, {}
    
    # 랩탑은 스윙이므로 ATR 2.0배수 손절선 계산
    base_sl_price = floor_to_tick(current_price - (atr * 2.0))
    if base_sl_price >= current_price: return False, {}
    
    # 🚨 [수정] 손절선 위 1.0% 고정 리스크 지정가 매수가 산출
    sl_pct = 1.0
    calc_entry_price = floor_to_tick(base_sl_price / (1 - (sl_pct / 100.0)))
    
    # 대기 매수가가 현재가보다 높으면 포기
    if calc_entry_price >= current_price: return False, {}
    
    # 진입가 기준 고정 리스크(%)를 금액(원)으로 환산하여 수량 계산
    risk_per_share = calc_entry_price - base_sl_price
    calc_qty = int(risk_amount // risk_per_share) if risk_per_share > 0 else 0
    if calc_qty <= 0: return False, {}

    # RR Ratio(손익비)에 따른 목표가 산출
    tp_price = ceil_to_tick(calc_entry_price + (risk_per_share * rr_ratio))

    return True, {
        'time': n1['time'],
        'entry_price': calc_entry_price, # 눌림목 대기 매수가
        'sl_price': base_sl_price,       # 최종 손절가
        'dynamic_tp': tp_price,          # 최종 익절가
        'qty': calc_qty, 
        'strategy': 'LAPTOP',
        'meta': {
            'pullback_vol_ratio': round(n2['volume'] / max(recent_10_vols), 2),
            'entry_atr': round(atr, 2),
            'strategy': 'LAPTOP'
        }
    }

# -----------------------------------------------------------------------------
# 4. 감시 리스트 및 미체결 동기화 모듈 (신규 추가)
# -----------------------------------------------------------------------------
def load_watch_list():
    """감시 종목 리스트를 로드합니다."""
    try:
        with open(WATCH_LIST_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.error(f"감시 리스트 로드 오류: {e}")
        return {}

def save_watch_list(data):
    """감시 종목 리스트를 저장합니다."""
    try:
        with open(WATCH_LIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"감시 리스트 저장 오류: {e}")

def sync_watch_list_with_balance(account_no):
    """
    현재 계좌 잔고와 '미체결 매수 내역'을 확인하여, 
    잔고에도 없고 매수 대기 중이지도 않은 종목만 감시 리스트에서 삭제합니다.
    """
    watch_list = load_watch_list()
    if not watch_list:
        return
        
    # 1. 잔고 조회 (실제 api 모듈의 메서드명과 응답 구조에 맞춰 조정 필요)
    balance_codes = []
    try:
        balance_data = kiwoom_api.get_balance(account_no)
        # 키움 REST API 응답 예시: data["body"]["output1"] 가 잔고 리스트라고 가정
        if balance_data and "body" in balance_data and "output1" in balance_data["body"]:
            balance_items = balance_data["body"]["output1"]
            balance_codes = [item.get("item_code") for item in balance_items if item.get("item_code")]
    except AttributeError:
        logger.warning("kiwoom_api.get_balance() 함수가 구현되지 않았습니다.")
        
    # 2. 미체결 매수 주문 내역 조회 (이전 단계에서 kiwoom_api에 추가하기로 한 함수)
    pending_buy_codes = []
    try:
        pending_buy_codes = kiwoom_api.get_unexecuted_orders(account_no)
    except AttributeError:
        logger.warning("kiwoom_api.get_unexecuted_orders() 함수가 구현되지 않았습니다.")
    
    # 3. 감시 리스트 검증 및 정리
    items_to_remove = []
    
    for code, info in watch_list.items():
        if code in balance_codes:
            # 잔고에 있음 -> 보유 상태로 업데이트
            if info.get("status") != "OWNED":
                info["status"] = "OWNED"
                logger.info(f"[{code}] 상태 변경: -> OWNED (잔고 확인)")
                
        elif code in pending_buy_codes:
            # 잔고에는 없지만 미체결 매수 대기 중 -> 삭제 방지 및 상태 업데이트
            if info.get("status") != "PENDING_BUY":
                info["status"] = "PENDING_BUY"
                logger.info(f"[{code}] 상태 변경: -> PENDING_BUY (미체결 매수 대기 중, 감시 유지)")
                
        else:
            # 잔고에도 없고 매수 대기 중이지도 않음 -> 삭제 대상
            items_to_remove.append(code)
            
    # 조건 미달 종목 삭제 실행
    for code in items_to_remove:
        del watch_list[code]
        logger.info(f"[{code}] 잔고 및 미체결 내역 없음. 감시 리스트에서 삭제됨.")
        
    save_watch_list(watch_list)