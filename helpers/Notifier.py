import requests
from dotenv import dotenv_values

config = dotenv_values(".env")

def Notifier(messa, phones = ['5492235385084']):
    for num in phones:
        message = {
            "phone_id_alias": config['AOKI_PHONE_ID_ALIAS'],
            "token": config['AOKI_BOTA_TOKEN'],
            "conversation": f"{num}",
            "text": messa
        }
        requests.post(url=f'{config["AOKI_URL"]}/api/v2/whatsapp/externalSend', data=message)
