import requests
import time
from config import telegram_token, telegram_chat_id

last_update_id = 0
URL = f"https://api.telegram.org/bot{telegram_token}"

def send_message(msg, reply_markup=None, reply_to_message_id=None):
    """일반 텍스트 및 버튼 메뉴 전송 (네트워크 단절 방어 및 지수 백오프 적용)"""
    url = f"{URL}/sendMessage"
    payload = {
        'chat_id': telegram_chat_id, 
        'text': msg,
        'parse_mode': 'Markdown'
    }
    if reply_markup:
        payload['reply_markup'] = reply_markup
    if reply_to_message_id:
        payload['reply_to_message_id'] = reply_to_message_id
        
    for attempt in range(5):  # 🚨 최대 5회 재시도 (방어력 강화)
        try:
            res = requests.post(url, json=payload, timeout=10)
            data = res.json()
            
            if data.get('ok'):
                return data['result'].get('message_id')
            else:
                err_desc = data.get('description', '알 수 없는 에러')
                print(f"⚠️ 텔레그램 텍스트 전송 거부: {err_desc}")
                
                # 마크다운 파싱 에러 감지 시, 일반 텍스트로 즉시 재시도
                if 'parse entities' in err_desc.lower() and 'parse_mode' in payload:
                    print("🔄 특수문자 마크다운 파싱 충돌 감지. 일반 텍스트 모드로 재전송합니다.")
                    del payload['parse_mode']
                    continue 
                break 
                
        except Exception as e:
            wait_time = 1.5 ** attempt  # 🚨 지수 백오프 (1.0초 -> 1.5초 -> 2.25초 ...)
            print(f"⚠️ 텔레그램 네트워크 연결 오류 (재시도 {attempt+1}/5, {wait_time:.1f}초 대기): {e}")
            time.sleep(wait_time)
            
    print("❌ 텔레그램 메시지 전송 최종 실패 (Silent Pass: 매매 엔진은 멈추지 않습니다)")
    return None

def edit_message_text(msg_id, msg, reply_markup=None):
    """기존 메시지 텍스트 갱신 (도배 방지용 라이브 대시보드)"""
    url = f"{URL}/editMessageText"
    payload = {
        'chat_id': telegram_chat_id,
        'message_id': msg_id,
        'text': msg,
        'parse_mode': 'Markdown'
    }
    if reply_markup:
        payload['reply_markup'] = reply_markup
        
    for attempt in range(5):
        try:
            res = requests.post(url, json=payload, timeout=10)
            data = res.json()
            if data.get('ok'):
                return True
            else:
                err_desc = data.get('description', '알 수 없는 에러')
                if 'parse entities' in err_desc.lower() and 'parse_mode' in payload:
                    del payload['parse_mode']
                    continue
                break
        except Exception as e:
            wait_time = 1.5 ** attempt
            print(f"⚠️ 텔레그램 메시지 수정 네트워크 오류 (재시도 {attempt+1}/5, {wait_time:.1f}초 대기): {e}")
            time.sleep(wait_time)
    return False

def pin_chat_message(msg_id):
    """중요 메시지 최상단 고정"""
    url = f"{URL}/pinChatMessage"
    payload = {
        'chat_id': telegram_chat_id,
        'message_id': msg_id,
        'disable_notification': True
    }
    for attempt in range(3):
        try:
            requests.post(url, json=payload, timeout=5)
            break
        except Exception:
            time.sleep(1)

def send_photo(photo_path, caption=""):
    """차트 이미지 전송 (네트워크 단절 방어 및 지수 백오프 적용)"""
    url = f"{URL}/sendPhoto"
    for attempt in range(5):
        try:
            with open(photo_path, 'rb') as photo:
                payload = {'chat_id': telegram_chat_id, 'caption': caption}
                files = {'photo': photo}
                requests.post(url, data=payload, files=files, timeout=30)
            break 
        except Exception as e:
            wait_time = 2 ** attempt
            print(f"⚠️ 텔레그램 사진 업로드 연결 오류 (재시도 {attempt+1}/5, {wait_time}초 대기): {e}")
            time.sleep(wait_time)

def answer_callback_query(callback_query_id):
    """버튼 클릭 로딩(스피너) 해제"""
    url = f"{URL}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception:
        pass 

def fetch_commands():
    """명령어 수신"""
    global last_update_id
    url = f"{URL}/getUpdates"
    params = {'offset': last_update_id, 'timeout': 1}
    commands = []
    
    try:
        response = requests.get(url, params=params, timeout=5).json()
        if response.get('ok'):
            for result in response['result']:
                last_update_id = result['update_id'] + 1
                
                text = result.get('message', {}).get('text', '').strip()
                if text:
                    commands.append(text)
                
                cb_query = result.get('callback_query', {})
                if cb_query:
                    cb_id = cb_query.get('id')
                    cb_data = cb_query.get('data', '').strip()
                    
                    if cb_id:
                        answer_callback_query(cb_id)
                        
                    if cb_data:
                        commands.append(f"cb:{cb_data}")
    except Exception as e:
        # 🚨 네트워크 단절 시 예외를 조용히 무시하고 1초 대기하여 무한 루프 과부하 방지
        time.sleep(1)
        
    return commands