import requests
import time
from config import telegram_token, telegram_chat_id

last_update_id = 0
URL = f"https://api.telegram.org/bot{telegram_token}"

def send_message(msg, reply_markup=None, reply_to_message_id=None):
    """일반 텍스트 및 버튼 메뉴 전송 (타임아웃 10초 확장 및 3회 재시도)"""
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
        
    for attempt in range(3):
        try:
            res = requests.post(url, json=payload, timeout=10)
            data = res.json()
            
            if data.get('ok'):
                return data['result'].get('message_id')
            else:
                # 🚨 텔레그램 서버가 전송을 거부했을 때의 방어 및 복구 로직
                err_desc = data.get('description', '알 수 없는 에러')
                print(f"⚠️ 텔레그램 텍스트 전송 거부: {err_desc}")
                
                # 마크다운 파싱 에러 감지 시, 일반 텍스트로 즉시 재시도
                if 'parse entities' in err_desc.lower() and 'parse_mode' in payload:
                    print("🔄 특수문자 마크다운 파싱 충돌 감지. 일반 텍스트 모드로 재전송합니다.")
                    del payload['parse_mode']
                    continue 
                break 
                
        except requests.exceptions.Timeout:
            print(f"⚠️ 텔레그램 텍스트 전송 지연 (재시도 {attempt+1}/3)")
            time.sleep(1) 
        except Exception as e:
            print(f"텔레그램 전송 실패: {e}")
            break
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
        
    for _ in range(3):
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
        except Exception:
            time.sleep(1)
    return False

def pin_chat_message(msg_id):
    """중요 메시지 최상단 고정"""
    url = f"{URL}/pinChatMessage"
    payload = {
        'chat_id': telegram_chat_id,
        'message_id': msg_id,
        'disable_notification': True
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception:
        pass

def send_photo(photo_path, caption=""):
    """차트 이미지 전송 (타임아웃 30초 확장 및 3회 재시도)"""
    url = f"{URL}/sendPhoto"
    for attempt in range(3):
        try:
            with open(photo_path, 'rb') as photo:
                payload = {'chat_id': telegram_chat_id, 'caption': caption}
                files = {'photo': photo}
                requests.post(url, data=payload, files=files, timeout=30)
            break 
        except requests.exceptions.Timeout:
            print(f"⚠️ 텔레그램 사진 업로드 지연 (재시도 {attempt+1}/3)")
            time.sleep(2) 
        except Exception as e:
            print(f"사진 전송 실패: {e}")
            break

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
    except Exception:
        pass 
        
    return commands