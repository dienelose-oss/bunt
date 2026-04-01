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
            # 🚨 대기 시간 5초 -> 10초로 연장
            res = requests.post(url, json=payload, timeout=10)
            data = res.json()
            if data.get('ok'):
                return data['result'].get('message_id')
            break # 성공 시 루프 즉시 탈출
        except requests.exceptions.Timeout:
            print(f"⚠️ 텔레그램 텍스트 전송 지연 (재시도 {attempt+1}/3)")
            time.sleep(1) # 1초 대기 후 재돌격
        except Exception as e:
            print(f"텔레그램 전송 실패: {e}")
            break
    return None

def send_photo(photo_path, caption=""):
    """차트 이미지 전송 (타임아웃 30초 확장 및 3회 재시도)"""
    url = f"{URL}/sendPhoto"
    for attempt in range(3):
        try:
            with open(photo_path, 'rb') as photo:
                payload = {'chat_id': telegram_chat_id, 'caption': caption}
                files = {'photo': photo}
                # 🚨 무거운 사진 대기 시간 10초 -> 30초로 연장
                requests.post(url, data=payload, files=files, timeout=30)
            break # 성공 시 루프 즉시 탈출
        except requests.exceptions.Timeout:
            print(f"⚠️ 텔레그램 사진 업로드 지연 (재시도 {attempt+1}/3)")
            time.sleep(2) # 2초 대기 후 재돌격
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
        pass # 수신 지연 시 조용히 무시 (엔진 속도 유지)
        
    return commands