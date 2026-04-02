import math

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
# 2. 제미나이 모멘텀 스캘핑 엔진 (자동 매매 - 코어 엔진)
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

    entry_price = n1['close']
    atr = calculate_atr(candles, 14)
    math_sl = entry_price * (1 - (sl_pct / 100.0))
    atr_sl = entry_price - (atr * 1.5) 
    
    sl_price = floor_to_tick(min(math_sl, atr_sl) if atr > 0 else math_sl)
    tp_price = ceil_to_tick(entry_price * (1 + (tp_pct / 100.0)))
    
    return True, {
        'time': n1['time'],
        'entry_price': entry_price,
        'sl_price': sl_price,
        'dynamic_tp': tp_price,
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
# 3. 랩탑 스윙 엔진 (VWAP + 9/20 EMA 추세추종 자동 매매)
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