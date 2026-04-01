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

# 🚨 [신규 필터 1] VWAP (당일 거래량 가중 평균가) 산출
def calculate_vwap(candles, today_str):
    cum_vol = 0
    cum_pv = 0
    # 과거부터 현재 순으로 누적 연산
    for c in reversed(candles):
        if c['time'].startswith(today_str):
            typ_price = (c['high'] + c['low'] + c['close']) / 3
            cum_vol += c['volume']
            cum_pv += typ_price * c['volume']
    return cum_pv / cum_vol if cum_vol > 0 else 0

# 🚨 [신규 필터 2] ATR (평균 진폭) 산출 - 변동성 측정용
def calculate_atr(candles, period=14):
    if len(candles) < period + 1: return 0
    trs = []
    for i in range(period):
        curr = candles[i]
        prev = candles[i+1]
        tr = max(curr['high'] - curr['low'], abs(curr['high'] - prev['close']), abs(curr['low'] - prev['close']))
        trs.append(tr)
    return sum(trs) / len(trs)

# 🚨 [신규 필터 3] MTF (다중 시간대 큰 추세) 확인
def check_long_trend(candles, period=60):
    if len(candles) < period: return True
    closes = [c['close'] for c in candles[:period]]
    sma = sum(closes) / len(closes)
    return candles[0]['close'] > sma

# -----------------------------------------------------------------------------
# 2. 제미나이 모멘텀 스캘핑 엔진 (자동 매매용)
# -----------------------------------------------------------------------------
def check_gemini_momentum_model(candles, today_str, tp_pct=1.5, sl_pct=1.0):
    if not candles or len(candles) < 60:
        return False, {}
    
    n1 = candles[1]
    
    if n1['close'] <= n1['open']:
        return False, {}
        
    recent_vols = [c['volume'] for c in candles[2:16]]
    avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
    
    if n1['volume'] < (avg_vol * 2.0):
        return False, {}

    # 🛡️ 팩트 방어막: 거시적 하락장이면 매수 기각
    if not check_long_trend(candles, 60):
        return False, {}

    # 🛡️ 팩트 방어막: 당일 VWAP(세력 평단) 아래면 매수 기각
    vwap = calculate_vwap(candles, today_str)
    if n1['close'] < vwap:
        return False, {}
        
    entry_price = n1['close']
    
    # 🛡️ ATR 기반 고무줄 손절선 (휩쏘 방어)
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
            'strategy': 'GEMINI'
        }
    }

# -----------------------------------------------------------------------------
# 3. 정통 오더블럭 장악형 스윙 엔진 (수동 타점용)
# -----------------------------------------------------------------------------
def check_orderblock_engulfing(candles, today_str, rr_ratio=1.5):
    if not candles or len(candles) < 60:
        return False, {}

    n1 = candles[1]  
    n2 = candles[2]  

    if n2['close'] >= n2['open']: 
        return False, {}

    if n1['close'] <= n2['open'] or n1['open'] > n2['close']: 
        return False, {}

    vol_n1 = n1['volume']
    vol_n2 = max(n2['volume'], 1)
    if vol_n1 < (vol_n2 * 1.5): 
        return False, {}

    recent_lows = [c['low'] for c in candles[2:17]]
    if n2['low'] > min(recent_lows): 
        return False, {}

    entry_price = n2['open'] 
    
    # 🛡️ 팩트 방어막: 타점이 당일 VWAP 아래면 기각 (역배열 칼날 잡기 방지)
    vwap = calculate_vwap(candles, today_str)
    if entry_price < vwap:
        return False, {}

    # 🛡️ 팩트 방어막: MTF 거시적 상승장 확인
    if not check_long_trend(candles, 60):
        return False, {}

    # 🛡️ ATR 휩쏘 방어막 주입
    atr = calculate_atr(candles, 14)
    base_sl = min(n1['low'], n2['low'])
    sl_price = floor_to_tick(base_sl - (atr * 0.5)) if atr > 0 else base_sl

    if entry_price <= sl_price: 
        return False, {}

    recent_highs = [c['high'] for c in candles[3:18]]
    swing_high = max(recent_highs)

    math_tp = entry_price + (entry_price - sl_price) * rr_ratio
    target_tp = min(swing_high, math_tp)

    risk = entry_price - sl_price
    reward = target_tp - entry_price
    if risk <= 0: return False, {}
    
    actual_rr = reward / risk
    if actual_rr < 1.0: 
        return False, {}

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
# 4. BPR / IFVG 모델 스윙 엔진 (수동 타점용)
# -----------------------------------------------------------------------------
def check_bpr_ifvg_model(candles, today_str, rr_ratio=1.5):
    if not candles or len(candles) < 60:
        return False, {}

    n1 = candles[1]
    n2 = candles[2]
    n3 = candles[3]

    date1 = n1['time'][:8]
    date2 = n2['time'][:8]
    date3 = n3['time'][:8]
    
    if not (date1 == date2 == date3):
        return False, {}

    if n1['low'] <= n3['high']:
        return False, {}
        
    fvg_top = n1['low']
    fvg_bottom = n3['high']

    vol_n2 = max(n2['volume'], 1)
    vol_n3 = max(n3['volume'], 1)
    
    if vol_n2 < (vol_n3 * 1.2): 
        return False, {}

    entry_price = fvg_top
    
    # 🛡️ VWAP 절대 지지선 팩트 체크
    vwap = calculate_vwap(candles, today_str)
    if entry_price < vwap:
        return False, {}

    # 🛡️ 큰 추세 정배열 확인
    if not check_long_trend(candles, 60):
        return False, {}

    # 🛡️ ATR 고무줄 손절 적용
    atr = calculate_atr(candles, 14)
    sl_price = floor_to_tick(fvg_bottom - (atr * 0.5)) if atr > 0 else fvg_bottom

    if entry_price <= sl_price:
        return False, {}

    recent_highs = [c['high'] for c in candles[1:15]]
    swing_high = max(recent_highs)

    math_tp = entry_price + (entry_price - sl_price) * rr_ratio
    target_tp = min(swing_high, math_tp)

    risk = entry_price - sl_price
    reward = target_tp - entry_price
    if risk <= 0: return False, {}
    
    actual_rr = reward / risk
    if actual_rr < 1.0: 
        return False, {}

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