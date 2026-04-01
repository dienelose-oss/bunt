import time
from datetime import datetime
import kiwoom_api
import strategy

def format_time_str(raw_time_str):
    if len(raw_time_str) >= 12:
        yy = raw_time_str[2:4]
        mm = raw_time_str[4:6]
        dd = raw_time_str[6:8]
        hh = raw_time_str[8:10]
        minute = raw_time_str[10:12]
        return f"{yy}년 {mm}월 {dd}일 {hh}시 {minute}분"
    return raw_time_str

def simulate_trade(candles_history, entry_price, sl_price, tp_price):
    is_entered = False
    
    for candle in candles_history:
        high = int(candle['high'])
        low = int(candle['low'])
        
        if not is_entered:
            if low <= entry_price:
                is_entered = True
                if low <= sl_price:
                    return "LOSS"
                if high >= tp_price:
                    return "WIN"
            continue 
            
        else:
            if low <= sl_price:
                return "LOSS"
            if high >= tp_price:
                return "WIN"
                
    if not is_entered:
        return "UNFILLED" 
        
    return "HOLD" 

def run_backtest(stock_codes, target_date, tic_scope='5'):
    
    # [수정됨] 회당 투자금 50만 원 세팅
    investment_per_trade = 500000  
    reward_ratio = 1.5               
    
    formatted_target_date = target_date
    if len(target_date) == 8:
        formatted_target_date = f"{target_date[2:4]}년 {target_date[4:6]}월 {target_date[6:8]}일"
    
    print(f"\n[백테스트 시작] 날짜: {formatted_target_date} / {tic_scope}분봉 / 대상: {len(stock_codes)}종목")
    print(f"▶ 진입 조건: 회당 {investment_per_trade:,}원 / 미체결 시 당일 종료 / 손익비 1:{reward_ratio}")
    print("-" * 65)
    
    total_signals = 0
    total_filled = 0
    total_wins = 0
    total_losses = 0
    cumulative_pnl = 0  
    
    for code in stock_codes:
        print(f"[{code}] 데이터 분석 중...")
        
        endpoint_candle = '/api/dostk/chart' 
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'authorization': f'Bearer {kiwoom_api.get_token()}',
            'cont-yn': 'N',
            'api-id': 'ka10080'
        }
        params = {
            'stk_cd': code,
            'tic_scope': str(tic_scope),
            'upd_stkpc_tp': '1', 
            'base_dt': target_date 
        }
        
        import requests
        try:
            res = requests.post(kiwoom_api.host_url + endpoint_candle, headers=headers, json=params, timeout=5).json()
            raw_candles = res.get('stk_min_pole_chart_qry', [])
        except Exception as e:
            continue
            
        if not raw_candles:
            continue
            
        parsed_candles = []
        for c in raw_candles:
            parsed_candles.append({
                'time': c.get('cntr_tm', ''),
                'open': int(c.get('open_pric', '0')),
                'close': int(c.get('cur_prc', '0')), 
                'high': int(c.get('high_pric', '0')),
                'low': int(c.get('low_pric', '0'))
            })
        parsed_candles.reverse() 
        
        for i in range(2, len(parsed_candles)):
            window = parsed_candles[i-2 : i+1]
            window.reverse() 
            
            is_ob, ob_data = strategy.check_order_block(window)
            
            if is_ob:
                total_signals += 1
                entry_time = format_time_str(ob_data['time'])
                
                entry_price = ob_data['entry_price']
                sl_price = ob_data['sl_price']
                
                risk_per_share = entry_price - sl_price
                if risk_per_share <= 0: continue
                
                qty = investment_per_trade // entry_price
                if qty == 0: continue
                
                tp_price = entry_price + int(risk_per_share * reward_ratio)
                future_candles = parsed_candles[i+1:]
                
                result = simulate_trade(future_candles, entry_price, sl_price, tp_price)
                
                trade_pnl = 0
                
                if result == "UNFILLED":
                    print(f"  - {entry_time} | 대기: {entry_price:,}원({qty}주) | ➖ 미체결 (매수 안 주고 상승)")
                    print(f"    🚫 [매매 종료] 첫 오더블럭 미체결로 인한 고점 추격매수 방지 (해당 종목 스캔 중지)")
                    # [핵심 로직] 미체결 시 해당 종목의 남은 캔들(다음 오더블럭) 검색을 즉시 중단합니다.
                    break 
                else:
                    total_filled += 1
                    if result == "WIN":
                        total_wins += 1
                        trade_pnl = qty * (tp_price - entry_price)
                        res_str = f"🟢 승리 (+{trade_pnl:,}원)"
                    elif result == "LOSS":
                        total_losses += 1
                        trade_pnl = -qty * risk_per_share
                        res_str = f"🔴 패배 ({trade_pnl:,}원)"
                    else:
                        res_str = "⚪ 홀딩 (0원)"
                
                    cumulative_pnl += trade_pnl
                    print(f"  - {entry_time} | 대기: {entry_price:,}원({qty}주) | {res_str}")
                    
                    # 체결되어 승패가 갈린 경우에도, 하루에 1번만 매매하고 싶다면 아래 주석을 해제하면 됩니다.
                    # break 
                
        time.sleep(0.5) 
        
    print("\n" + "=" * 65)
    print("📊 [백테스트 최종 결과 리포트]")
    print(f"• 포착된 총 신호: {total_signals}회")
    print(f"• 실제 매수 체결: {total_filled}회 (미체결 후 스캔 종료 {total_signals - total_filled}회)")
    print(f"• 승리(익절): {total_wins}회")
    print(f"• 패배(손절): {total_losses}회")
    
    if total_filled > 0:
        win_rate = (total_wins / (total_wins + total_losses)) * 100 if (total_wins + total_losses) > 0 else 0
        print(f"• 체결 대비 승률: {win_rate:.2f}%")
        
        pnl_color = "🔴" if cumulative_pnl < 0 else "🟢"
        sign = "+" if cumulative_pnl > 0 else ""
        print(f"• 최종 누적 손익금: {pnl_color} {sign}{cumulative_pnl:,}원")
    else:
        print("• 체결된 타점이 없습니다.")
    print("=" * 65)

if __name__ == '__main__':
    test_date = '20260313' 
    
    # [수정됨] 검증용 종목코드 세팅 (272210)
    sample_codes = [
        '003280' 
    ]
    
    run_backtest(sample_codes, test_date, tic_scope='5')