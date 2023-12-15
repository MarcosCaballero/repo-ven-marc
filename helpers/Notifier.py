import requests


def Notifier(messa, phones = ['5492235385084']):
    for num in phones:
        message = {
            "phone": f"{num}",
            "text": messa
        }
        requests.post(url='https://api-bota.aokitech.com.ar/api/v2/whatsapp/raw-message', data=message)
