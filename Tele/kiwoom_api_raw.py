import time
import aiohttp
from datetime import datetime, time as dt_time
from config import host_url
from login import fn_au10001 as get_token

_CACHED_TOKEN = None
_TOKEN_EXPIRY = 0

def get_valid_token():
    global _CACHED_TOKEN, _TOKEN_EXPIRY
    current_time = time.time()
    
    if _CACHED_TOKEN is None or current_time > _TOKEN_EXPIRY:
        _CACHED_TOKEN = get_token()
        _TOKEN_EXPIRY = current_time + 21600  # 6시간 캐싱
        
    return _CACHED_TOKEN

async def _request_api(session, endpoint, api_id, params, use_get=False):
    url = host_url + endpoint
    now_time = datetime.now().time()
    
    is_nxt = (dt_time(8, 0) <= now_time < dt_time(8, 50)) or (dt_time(15, 40) <= now_time <= dt_time(20, 0))
    exchange_tp = 'NXT' if is_nxt else 'KRX'
    
    if 'dmst_stex_tp' in params and params['dmst_stex_tp'] == 'AUTO':
        params['dmst_stex_tp'] = exchange_tp

    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {get_valid_token()}',
        'cont-yn': 'N',
        'api-id': api_id,
    }

    try:
        if use_get:
            async with session.get(url, headers=headers, params=params, timeout=5) as response:
                return await response.json(), exchange_tp
        else:
            async with session.post(url, headers=headers, json=params, timeout=5) as response:
                return await response.json(), exchange_tp
    except Exception as e:
        print(f"API 통신 오류 [{api_id}]: {e}")
        return None, exchange_tp

async def get_estimated_assets(session):
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'})
    if not res or ('return_code' in res and str(res['return_code']) != '0'):
        return None
    return int(res.get('prsm_dpst_aset_amt', res.get('tot_est_amt', '0')))

async def get_orderable_cash(session):
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'})
    if not res or ('return_code' in res and str(res['return_code']) != '0'):
        return 0
    return int(res.get('d2_entra', '0'))

async def get_account_balance(session):
    res, extp = await _request_api(session, '/api/dostk/acnt', 'kt00004', {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'})
    if not res or ('return_code' in res and str(res['return_code']) != '0'):
        return f"❌ 잔고 조회 실패: {res.get('return_msg', '응답 없음') if res else '통신오류'}"

    d2_deposit = int(res.get('d2_entra', '0'))
    estimated_assets = int(res.get('prsm_dpst_aset_amt', '0'))
    total_profit = int(res.get('lspft', '0'))
    profit_rate = float(res.get('lspft_rt', '0'))
    
    msg = f"📊 [현재 계좌 잔고]\n"
    msg += f"• 추정자산: {estimated_assets:,}원\n"
    msg += f"• D+2 예수금: {d2_deposit:,}원\n"
    
    sign = "+" if total_profit > 0 else ""
    msg += f"• 누적 손익: {sign}{total_profit:,}원 ({sign}{profit_rate}%)\n\n"
    msg += f"📋 [보유 종목 현황]\n"
    
    valid_count = 0
    for stk in res.get('stk_acnt_evlt_prst', []):
        qty = int(stk.get('rmnd_qty', '0'))
        if qty > 0:
            valid_count += 1
            name = stk.get('stk_nm', '').strip()
            pl_amt = int(stk.get('pl_amt', '0'))
            pl_rt = float(stk.get('pl_rt', '0'))
            s_sign = "+" if pl_amt > 0 else ""
            msg += f"• {name} : {qty:,}주 ({s_sign}{pl_rt}%, {s_sign}{pl_amt:,}원)\n"
            
    if valid_count > 0:
        return msg
    else:
        return msg + "보유 중인 종목이 없습니다."

async def get_holdings_data(session):
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'})
    if not res or str(res.get('return_code', '1')) != '0':
        return None

    holdings = {}
    for stk in res.get('stk_acnt_evlt_prst', []):
        qty = int(stk.get('rmnd_qty', '0'))
        if qty > 0:
            raw_code = stk.get('pdno', stk.get('stk_cd', '')).strip()
            code = raw_code[1:] if raw_code.startswith('A') else raw_code
            name = stk.get('stk_nm', '').strip()
            prpr = int(stk.get('prpr', '0'))
            
            if prpr == 0 and int(stk.get('evlt_amt', '0')) > 0:
                prpr = int(stk.get('evlt_amt', '0')) // qty
                
            holdings[code] = {'name': name, 'prpr': prpr, 'qty': qty}
            
    return holdings

async def buy_limit_order(session, stock_code, qty, price):
    params = {
        'dmst_stex_tp': 'AUTO', 
        'stk_cd': stock_code, 
        'ord_qty': str(qty), 
        'ord_uv': str(price), 
        'trde_tp': '00', 
        'ord_tp': '1', 
        'cond_uv': '0'
    }
    res, _ = await _request_api(session, '/api/dostk/ordr', 'kt10000', params)
    
    if res and str(res.get('return_code', res.get('rt_cd', '1'))) == '0':
        output = res.get('output', {})
        odno = output.get('ODNO', output.get('odno', res.get('ODNO', res.get('odno', ''))))
        
        if not odno:
            odno = output.get('ord_no', output.get('ordNo', res.get('ord_no', '')))
            
        if not odno:
            raw_data = str(res)[:200]
            return f"⚠️ [{stock_code}] {qty}주 매수 접수 성공.\n(경고: 주문번호 추출 실패. 데이터: {raw_data})", ""
            
        return f"✅ [{stock_code}] {qty}주 / {price:,}원 지정가 매수 접수. (ODNO: {odno})", odno
        
    return f"❌ 매수 실패: {res.get('return_msg', res.get('msg1', '응답없음'))}", None

async def cancel_order(session, stock_code, orgn_odno):
    params = {
        'dmst_stex_tp': 'AUTO', 
        'stk_cd': stock_code, 
        'ord_qty': '0', 
        'ord_uv': '0', 
        'trde_tp': '00', 
        'ord_tp': '3',
        'RVSE_CNCL_DVSN_CD': '02', 
        'cond_uv': '0',
        'orgn_odno': str(orgn_odno),     
        'orig_ord_no': str(orgn_odno)    
    }
    res, _ = await _request_api(session, '/api/dostk/ordr', 'kt10002', params)
    
    if res and str(res.get('return_code', res.get('rt_cd', '1'))) == '0':
        return f"✅ [{stock_code}] 미체결 취소 완료"
    return f"❌ 취소 실패: {res.get('return_msg', res.get('msg1', '오류'))}"

async def sell_market_order(session, stock_code, qty):
    params = {
        'dmst_stex_tp': 'AUTO', 
        'stk_cd': stock_code, 
        'ord_qty': str(qty), 
        'ord_uv': '0', 
        'trde_tp': '01', 
        'ord_tp': '2', 
        'cond_uv': '0'
    }
    res, _ = await _request_api(session, '/api/dostk/ordr', 'kt10001', params)
    
    if res and str(res.get('return_code')) == '0':
        return f"✅ [{stock_code}] {qty}주 시장가 매도 완료"
        
    return f"❌ 매도 실패: [{res.get('return_code') if res else '오류'}]({res.get('return_msg') if res else '응답없음'})"

async def sell_limit_order(session, stock_code, qty, price):
    params = {
        'dmst_stex_tp': 'AUTO', 
        'stk_cd': stock_code, 
        'ord_qty': str(qty), 
        'ord_uv': str(int(price)), 
        'trde_tp': '00', 
        'ord_tp': '2', 
        'cond_uv': '0'
    }
    res, _ = await _request_api(session, '/api/dostk/ordr', 'kt10001', params)
    
    if res and str(res.get('return_code')) == '0':
        return f"✅ [{stock_code}] {qty}주 지정가({int(price):,}원) 강제 투매 완료"
        
    return f"❌ 2차 매도 실패: [{res.get('return_code') if res else '오류'}]({res.get('return_msg') if res else '응답없음'})"

async def get_top_20_search_rank(session):
    res, _ = await _request_api(session, '/api/dostk/stkinfo', 'ka00198', {'qry_tp': '1'})
    if not res: 
        return {}
        
    result = {}
    for stk in res.get('item_inq_rank', [])[:20]:
        code = stk.get('stk_cd', '')
        if code:
            result[code] = stk.get('stk_nm', '')
            
    return result
async def get_candles(session, stock_code, tic_scope):
    params = {
        'stk_cd': stock_code, 
        'tic_scope': str(tic_scope), 
        'upd_stkpc_tp': '1', 
        'base_dt': datetime.now().strftime('%Y%m%d')
    }
    res, _ = await _request_api(session, '/api/dostk/chart', 'ka10080', params)
    if not res: 
        return "", []

    name = res.get('stk_nm', '').strip()
    parsed = []
    for c in res.get('stk_min_pole_chart_qry', []):
        try: 
            vol = int(str(c.get('trde_qty', '0')).replace(',', '').replace('+', '').replace('-', ''))
        except: 
            vol = 0
            
        parsed.append({
            'time': c.get('cntr_tm', ''), 
            'open': abs(int(str(c.get('open_pric', '0')).strip() or '0')),
            'close': abs(int(str(c.get('cur_prc', '0')).strip() or '0')), 
            'high': abs(int(str(c.get('high_pric', '0')).strip() or '0')),
            'low': abs(int(str(c.get('low_pric', '0')).strip() or '0')), 
            'volume': vol
        })
    return parsed

async def get_orderbook(session, stock_code):
    params = {
        'stk_cd': stock_code, 
        'base_dt': datetime.now().strftime('%Y%m%d')
    }
    res, _ = await _request_api(session, '/api/dostk/hoga', 'ka10081', params)
    if not res: 
        return 0, 0
        
    ask_vol = int(str(res.get('tot_sell_ho_remn', '0')).strip() or '0')
    bid_vol = int(str(res.get('tot_buy_ho_remn', '0')).strip() or '0')
    
    return ask_vol, bid_vol