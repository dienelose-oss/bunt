import time
import aiohttp
import asyncio
import json
from datetime import datetime, time as dt_time
from config import host_url
from login import fn_au10001 as get_token

# --- 웹소켓 전용 전역 변수 ---
realtime_prices = {}
ws_client = None
_ws_approval_key = None

def get_valid_token():
    # B Code와 동일하게 매 통신마다 최신/유효 토큰을 가져오도록 롤백
    return get_token()

async def _request_api(session, endpoint, api_id, params, use_get=False):
    url = host_url + endpoint
    now_time = datetime.now().time()
    
    is_nxt = (dt_time(8, 0) <= now_time < dt_time(8, 50)) or (dt_time(15, 40) <= now_time <= dt_time(20, 0))
    exchange_tp = 'NXT' if is_nxt else 'KRX'
    
    if 'dmst_stex_tp' in params and params['dmst_stex_tp'] == 'AUTO':
        params['dmst_stex_tp'] = exchange_tp

    # 🚨 원래대로 롤백 (순정 헤더 상태 유지)
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {get_valid_token()}',
        'cont-yn': 'N',
        'api-id': api_id,
    }

    # 🚨 지능형 스로틀링 (Rate Limit 방어 및 재시도)
    retry_count = 0
    while retry_count < 3:
        try:
            if use_get:
                async with session.get(url, headers=headers, params=params, timeout=5) as response:
                    res = await response.json(content_type=None)
            else:
                async with session.post(url, headers=headers, json=params, timeout=5) as response:
                    res = await response.json(content_type=None)
                    
            if res and str(res.get('return_code', res.get('rt_cd', ''))) in ['1700', '1687']:
                print(f"API 호출 제한(Rate Limit) 감지! 1초 대기 후 재시도... ({retry_count+1}/3)")
                await asyncio.sleep(1)
                retry_count += 1
                continue
                
            return res, exchange_tp
        except Exception as e:
            print(f"API 통신 오류 [{api_id}]: {e}")
            await asyncio.sleep(0.5)
            retry_count += 1
            
    return None, exchange_tp

# --- 실시간 웹소켓(WebSocket) 매니저 클래스 ---
async def init_websocket_keys(session):
    global _ws_approval_key
    try:
        import config
        app_key = getattr(config, 'app_key', getattr(config, 'APP_KEY', ''))
        app_secret = getattr(config, 'app_secret', getattr(config, 'APP_SECRET', ''))
        h_url = getattr(config, 'host_url', getattr(config, 'HOST_URL', ''))
        
        url = h_url + '/oauth2/Approval'
        data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
        async with session.post(url, json=data) as res:
            rj = await res.json(content_type=None)
            _ws_approval_key = rj.get('approval_key')
    except Exception as e:
        print(f"웹소켓 키 초기화 실패 (config.py 확인 요망): {e}")

class KISWebSocket:
    def __init__(self, session):
        self.session = session
        self.ws = None
        self.subscribed = set()
        self.is_running = False

    async def connect(self):
        if not _ws_approval_key:
            await init_websocket_keys(self.session)
        if not _ws_approval_key: return

        import config
        h_url = getattr(config, 'host_url', getattr(config, 'HOST_URL', ''))
        ws_url = "ws://ops.koreainvestment.com:21000"
        if "vps" in h_url: ws_url = "ws://ops.koreainvestment.com:31000"
        
        try:
            self.ws = await self.session.ws_connect(ws_url, ping_interval=60)
            self.is_running = True
            asyncio.create_task(self._listen())
            print("🚀 [시스템] 실시간 웹소켓 틱 데이터 스트리밍 연결 완료.")
        except Exception as e:
            print(f"WS 연결 에러: {e}")

    async def _listen(self):
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.data
                    if data[0] == '0' or data[0] == '1': # 실시간 체결가 데이터 수신
                        parts = data.split('|')
                        if len(parts) >= 4:
                            code_data = parts[3].split('^')
                            if len(code_data) >= 2:
                                code = code_data[0]
                                price = abs(int(code_data[1]))
                                realtime_prices[code] = price
        except Exception as e:
            print(f"웹소켓 수신 에러: {e}")
        finally:
            if self.is_running:
                print("⚠️ 웹소켓 연결이 예기치 않게 종료되었습니다. 자동 재연결이 필요합니다.")
                self.is_running = False

    async def subscribe(self, code):
        if code in self.subscribed or not self.is_running: return
        req = {
            "header": {"approval_key": _ws_approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
            "body": {"input": {"tr_id": "H0STCNT0", "tr_key": code}}
        }
        await self.ws.send_json(req)
        self.subscribed.add(code)

    async def unsubscribe(self, code):
        if code not in self.subscribed or not self.is_running: return
        req = {
            "header": {"approval_key": _ws_approval_key, "custtype": "P", "tr_type": "2", "content-type": "utf-8"},
            "body": {"input": {"tr_id": "H0STCNT0", "tr_key": code}}
        }
        await self.ws.send_json(req)
        self.subscribed.remove(code)
        if code in realtime_prices:
            del realtime_prices[code]

async def get_estimated_assets(session):
    params = {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'}
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', params) 
    
    if not res or str(res.get('rt_cd', res.get('return_code', '1'))) != '0':
        return None
        
    out2 = res.get('output2', [{}])
    summary = out2[0] if isinstance(out2, list) and len(out2) > 0 else out2
    if not summary: summary = res
    
    return int(summary.get('tot_evlu_amt', summary.get('prsm_dpst_aset_amt', '0')))

async def get_orderable_cash(session):
    params = {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'}
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', params) 
    
    if not res or str(res.get('rt_cd', res.get('return_code', '1'))) != '0':
        return 0
        
    out2 = res.get('output2', [{}])
    summary = out2[0] if isinstance(out2, list) and len(out2) > 0 else out2
    if not summary: summary = res
    
    return int(summary.get('prvs_rcdl_excc_amt', summary.get('d2_entra', '0')))

async def get_account_balance(session):
    params = {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'}
    res, extp = await _request_api(session, '/api/dostk/acnt', 'kt00004', params) 
    
    if not res:
        return "❌ 잔고 조회 실패: 통신오류 (None)"

    # 🚨 원본 JSON 덤프 강제 출력 모드 (데이터 구조 파악용)
    dump_msg = f"🛠️ [데이터 구조 파악용 원본 덤프]\n아래 내용을 복사해서 개발자에게 알려주세요:\n\n{json.dumps(res, ensure_ascii=False, indent=2)[:3500]}"
    return dump_msg

async def get_holdings_data(session):
    params = {'qry_tp': '0', 'dmst_stex_tp': 'AUTO'}
    res, _ = await _request_api(session, '/api/dostk/acnt', 'kt00004', params) 
    
    if not res or str(res.get('rt_cd', res.get('return_code', '1'))) != '0':
        return None

    holdings = {}
    out1 = res.get('output1', res.get('stk_acnt_evlt_prst', []))
    for stk in out1:
        qty = int(stk.get('hldg_qty', stk.get('rmnd_qty', '0')))
        if qty > 0:
            raw_code = stk.get('pdno', stk.get('stk_cd', '')).strip()
            code = raw_code[1:] if raw_code.startswith('A') else raw_code
            name = stk.get('prdt_name', stk.get('stk_nm', '')).strip()
            prpr = int(stk.get('prpr', '0'))
            
            if prpr == 0 and int(stk.get('evlu_amt', stk.get('evlt_amt', '0'))) > 0:
                prpr = int(stk.get('evlu_amt', stk.get('evlt_amt', '0'))) // qty
                
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
    
    if res and str(res.get('rt_cd', res.get('return_code', '1'))) == '0':
        output = res.get('output', {})
        odno = output.get('ODNO', output.get('odno', res.get('ODNO', res.get('odno', ''))))
        
        if not odno:
            odno = output.get('ord_no', output.get('ordNo', res.get('ord_no', '')))
            
        if not odno:
            raw_data = str(res)[:200]
            return f"⚠️ [{stock_code}] {qty}주 매수 접수 성공.\n(경고: 주문번호 추출 실패. 데이터: {raw_data})", ""
            
        return f"✅ [{stock_code}] {qty}주 / {price:,}원 지정가 매수 접수. (ODNO: {odno})", odno
        
    return f"❌ 매수 실패: {res.get('msg1', res.get('return_msg', '응답없음'))}", None

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
    
    if res and str(res.get('rt_cd', res.get('return_code', '1'))) == '0':
        return f"✅ [{stock_code}] 미체결 취소 완료"
    return f"❌ 취소 실패: {res.get('msg1', res.get('return_msg', '오류'))}"

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
    
    if res and str(res.get('rt_cd', res.get('return_code', '1'))) == '0':
        return f"✅ [{stock_code}] {qty}주 시장가 매도 완료"
        
    return f"❌ 매도 실패: 에러코드 {res.get('rt_cd', '오류')} / 사유: {res.get('msg1', res.get('return_msg', '응답없음'))}"

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
    
    if res and str(res.get('rt_cd', res.get('return_code', '1'))) == '0':
        return f"✅ [{stock_code}] {qty}주 지정가({int(price):,}원) 매도 접수 완료"
        
    return f"❌ 2차 매도 실패: 에러코드 {res.get('rt_cd', '오류')} / 사유: {res.get('msg1', res.get('return_msg', '응답없음'))}"

async def get_top_20_search_rank(session):
    res, _ = await _request_api(session, '/api/dostk/stkinfo', 'ka00198', {'qry_tp': '1'}) 
    if not res: 
        return {}
        
    result = {}
    for stk in res.get('output', res.get('item_inq_rank', []))[:20]:
        code = stk.get('stk_cd', stk.get('pdno', ''))
        if code:
            result[code] = stk.get('stk_nm', stk.get('prdt_name', ''))
            
    return result

async def get_top_20_volume_rank(session):
    res, _ = await _request_api(session, '/api/dostk/stkinfo', 'ka00216', {'qry_tp': '1', 'dmst_stex_tp': 'AUTO'}) 
    if not res: 
        return {}
        
    result = {}
    for stk in res.get('output', res.get('trde_qty_rank', []))[:20]:
        code = stk.get('stk_cd', stk.get('pdno', ''))
        if code:
            result[code] = stk.get('stk_nm', stk.get('prdt_name', '')).strip()
            
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
        return []

    parsed = []
    for c in res.get('output2', res.get('stk_min_pole_chart_qry', [])):
        try: 
            vol = int(str(c.get('trde_qty', c.get('acml_vol', '0'))).replace(',', '').replace('+', '').replace('-', ''))
        except: 
            vol = 0
            
        parsed.append({
            'time': c.get('cntr_tm', c.get('stck_cntg_hour', '')), 
            'open': abs(int(str(c.get('open_pric', c.get('stck_oprc', '0'))).strip() or '0')),
            'close': abs(int(str(c.get('cur_prc', c.get('stck_prpr', '0'))).strip() or '0')), 
            'high': abs(int(str(c.get('high_pric', c.get('stck_hgpr', '0'))).strip() or '0')),
            'low': abs(int(str(c.get('low_pric', c.get('stck_lwpr', '0'))).strip() or '0')), 
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
        return 0, 0, 0, 0
        
    out1 = res.get('output1', res)
    ask_vol = int(str(out1.get('tot_sell_ho_remn', '0')).strip() or '0')
    bid_vol = int(str(out1.get('tot_buy_ho_remn', '0')).strip() or '0')
    ask_price = int(str(out1.get('sell_ho_prc1', '0')).strip() or '0')
    bid_price = int(str(out1.get('buy_ho_prc1', '0')).strip() or '0')
    
    return ask_vol, bid_vol, ask_price, bid_price