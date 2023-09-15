import requests


def Notifier(messa):
    numbers = ['5492235385084']

    for num in numbers:
        message = {
            "phone": f"{num}",
            "text": messa
        }
        requests.post(url='https://api-distri-laravel.rj.r.appspot.com/api/v2/whatsapp/raw-message', data=message)
