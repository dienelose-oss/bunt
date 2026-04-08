import math
from collections import deque
from datetime import datetime

# -----------------------------------------------------------------------------
# 0. 인메모리 캔들 엔진 (웹소켓 틱 기반 실시간 연산)
# -----------------------------------------------------------------------------
class CandleManager:
    def __init__(self, max_len=100):
        self.candles = {}  # {code: deque}
        self.max_len = max_len

    def init_stock(self, code, past_candles):
        if past_candles:
            self.candles[code] = deque(past_candles[-self.max_len:], maxlen=self.max_len)

    def update_tick(self, code, price, volume=0):
        if code not in self.candles or not self.candles[code]: return
        now_str = datetime.now().strftime('%Y%m%d%H%M%S')
        last = self.candles[code][-1]
        
        # HHMM 기준 분이 변경되면 새 캔들을 생성하여 이음
        if last['time'][8:12] != now_str[8:12]:
            new_c = {'time': now_str, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': volume}
            self.candles[code].append(new_c)
        else:
            last['close'] = price
            if price > last['high']: last['high'] = price
            if price < last['low']: last['low'] = price
            last['volume'] += volume

    def get_ma(self, code, period):
        if code not in self.candles or len(self.candles[code]) < period: return 0
        closes = [c['close'] for c in list(self.candles[code])[-period:]]
        return sum(closes) / period

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

def optimize_entry_with_orderbook(base_entry, ask_vol, bid_vol):
    if bid_vol > ask_vol * 1.5:
        return base_entry + get_tick_size(base_entry)
    return base_entry

def calculate_trailing_stop(rt_price, current_sl, max_reached):
    drop_limit = max_reached * 0.98
    new_sl = max(current_sl, floor_to_tick(drop_limit))
    return new_sl, max_reached

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
# 2. 제미나이 모멘텀 스캘핑 엔진 (자동 매매 - 진단 보고 기능 탑재)
# -----------------------------------------------------------------------------
def check_gemini_momentum_model(candles, today_str, tp_pct=1.5, sl_pct=1.0, filter_lvl=3):
    if not candles or len(candles) < 60: return False, {}
    n1 = candles[1]
    
    # [Lv.1 공통 조건] 양봉 및 거래량 2배 폭발 확인
    if n1['close'] <= n1['open']: return False, {}
        
    recent_vols = [c['volume'] for c in candles[2:16]]
    avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
    vol_burst_ratio = n1['volume'] / avg_vol

    if vol_burst_ratio < 2.0: return False, {}
    
    # 🚨 [신규 추가] VolBurst 킬 스위치 (거래량 15배 이상 폭발 시 작전/설거지 의심으로 진입 전면 차단)
    if vol_burst_ratio > 15.0: return False, {}

    # 🚨 [팩트 반영] 상위 필터 조건 무조건 사전 계산
    vwap = calculate_vwap(candles, today_str)
    is_above_vwap = (n1['close'] >= vwap)
    is_long_trend = check_long_trend(candles, 60)

    # 설정된 레벨에 따라 진입 차단
    if filter_lvl >= 2 and not is_above_vwap: return False, {}
    if filter_lvl >= 3 and not is_long_trend: return False, {}
    
    # 🚨 [추가된 로직] Safe-Zone: 폭발적인 빔을 쐈으나 이미 VWAP 대비 3% 초과 상승했다면 위험지대로 간주
    fail_reasons = []
    if not is_above_vwap: fail_reasons.append("VWAP 하회(Lv.2 미달)")
    if not is_long_trend: fail_reasons.append("60선 역배열(Lv.3 미달)")
    if is_above_vwap and n1['close'] > vwap * 1.03: 
        fail_reasons.append("VWAP 3% 이상 초과(추격매수 위험)")
        return False, {}

    diag_msg = " + ".join(fail_reasons) if fail_reasons else "모든 조건 충족(Lv.3급 완벽)"

    entry_price = n1['close']
    atr = calculate_atr(candles, 14)
    
    # ATR 변동성 여유폭 로직 삭제 - 무조건 설정값(math_sl) 기준 칼손절
    math_sl = entry_price * (1 - (sl_pct / 100.0))
    sl_price = floor_to_tick(math_sl)
    
    tp_price = ceil_to_tick(entry_price * (1 + (tp_pct / 100.0)))
    
    return True, {
        'time': n1['time'],
        'entry_price': entry_price,
        'sl_price': sl_price,
        'dynamic_tp': tp_price,
        'strategy': 'GEMINI',
        'meta': {
            'vol_burst_ratio': round(vol_burst_ratio, 2),
            'entry_atr': round(atr, 2),
            'upper_tail_ratio': 0,
            'strategy': 'GEMINI',
            'diag_msg': diag_msg
        }
    }

# -----------------------------------------------------------------------------
# 3. 랩탑 스윙 엔진 (VWAP + 9/20 EMA 추세추종 자동 매매)
# -----------------------------------------------------------------------------
def check_laptop_swing_model(candles, today_str, risk_amount, rr_ratio=2.0):
    if not candles or len(candles) < 40: return False, {}
    
    n1 = candles[1] 
    n2 = candles[2] 
    
    if not check_long_trend(candles, 40): return False, {}

    vwap = calculate_vwap(candles, today_str)
    if n1['close'] < vwap or n2['close'] < vwap: return False, {}
    
    # 🚨 [추가된 로직] Safe-Zone: 스윙 특성상 안전마진 확보를 위해 VWAP과 2% 이상 벌어진 타점 차단
    if n1['close'] > vwap * 1.02: return False, {}
    
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

    entry_price = n1['close']
    atr = calculate_atr(candles, 14)
    
    sl_price = floor_to_tick(entry_price - (atr * 2.0))
    if sl_price >= entry_price: return False, {}
    
    risk = entry_price - sl_price
    calc_qty = int(risk_amount // risk) if risk > 0 else 0
    if calc_qty <= 0: return False, {}

    tp_price = ceil_to_tick(entry_price + (risk * rr_ratio))

    return True, {
        'time': n1['time'],
        'entry_price': entry_price,
        'sl_price': sl_price,
        'dynamic_tp': tp_price,
        'qty': calc_qty, 
        'strategy': 'LAPTOP',
        'meta': {
            'pullback_vol_ratio': round(n2['volume'] / max(recent_10_vols), 2),
            'entry_atr': round(atr, 2),
            'strategy': 'LAPTOP'
        }
    }

# -----------------------------------------------------------------------------
# 4. 정통 오더블럭 장악형 스윙 엔진 (수동)
# -----------------------------------------------------------------------------
def check_orderblock_engulfing(candles, today_str, rr_ratio=1.5):
    if not candles or len(candles) < 60: return False, {}
    n1 = candles[1]  
    n2 = candles[2]  

    if n2['close'] >= n2['open']: return False, {}
    if n1['close'] <= n2['open'] or n1['open'] > n2['close']: return False, {}

    vol_n1 = n1['volume']
    vol_n2 = max(n2['volume'], 1)
    if vol_n1 < (vol_n2 * 1.5): return False, {}

    recent_lows = [c['low'] for c in candles[2:17]]
    if n2['low'] > min(recent_lows): return False, {}

    entry_price = n2['open'] 
    
    vwap = calculate_vwap(candles, today_str)
    if entry_price < vwap: return False, {}

    if not check_long_trend(candles, 60): return False, {}

    atr = calculate_atr(candles, 14)
    base_sl = min(n1['low'], n2['low'])
    sl_price = floor_to_tick(base_sl - (atr * 0.5)) if atr > 0 else base_sl
    if entry_price <= sl_price: return False, {}

    recent_highs = [c['high'] for c in candles[3:18]]
    swing_high = max(recent_highs)

    math_tp = entry_price + (entry_price - sl_price) * rr_ratio
    target_tp = min(swing_high, math_tp)

    risk = entry_price - sl_price
    reward = target_tp - entry_price
    if risk <= 0: return False, {}
    
    actual_rr = reward / risk
    if actual_rr < 1.0: return False, {}

    return True, {
        'time': n1['time'],
        'entry_price': entry_price,
        'sl_price': sl_price,
        'dynamic_tp': int(target_tp),
        'strategy': 'OB',
        'meta': {
            'vol_ratio': round(vol_n1 / vol_n2, 2),
            'actual_rr': round(actual_rr, 2),
            'strategy': 'OB'
        }
    }

# -----------------------------------------------------------------------------
# 5. BPR / IFVG 모델 스윙 엔진 (수동)
# -----------------------------------------------------------------------------
def check_bpr_ifvg_model(candles, today_str, rr_ratio=1.5):
    if not candles or len(candles) < 60: return False, {}

    n1 = candles[1]
    n2 = candles[2]
    n3 = candles[3]

    date1 = n1['time'][:8]
    date2 = n2['time'][:8]
    date3 = n3['time'][:8]
    if not (date1 == date2 == date3): return False, {}

    if n1['low'] <= n3['high']: return False, {}
        
    fvg_top = n1['low']
    fvg_bottom = n3['high']

    vol_n2 = max(n2['volume'], 1)
    vol_n3 = max(n3['volume'], 1)
    if vol_n2 < (vol_n3 * 1.2): return False, {}

    entry_price = fvg_top
    vwap = calculate_vwap(candles, today_str)
    if entry_price < vwap: return False, {}

    if not check_long_trend(candles, 60): return False, {}

    atr = calculate_atr(candles, 14)
    sl_price = floor_to_tick(fvg_bottom - (atr * 0.5)) if atr > 0 else fvg_bottom
    if entry_price <= sl_price: return False, {}

    recent_highs = [c['high'] for c in candles[1:15]]
    swing_high = max(recent_highs)

    math_tp = entry_price + (entry_price - sl_price) * rr_ratio
    target_tp = min(swing_high, math_tp)

    risk = entry_price - sl_price
    reward = target_tp - entry_price
    if risk <= 0: return False, {}
    
    actual_rr = reward / risk
    if actual_rr < 1.0: return False, {}

    return True, {
        'time': n1['time'],
        'entry_price': entry_price,
        'sl_price': sl_price,
        'dynamic_tp': int(target_tp),
        'strategy': 'BPR',
        'meta': {
            'vol_ratio': round(vol_n2 / vol_n3, 2),
            'actual_rr': round(actual_rr, 2),
            'strategy': 'BPR'
        }
    }