import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'x-my-custom-header': 'Index',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'http://www.fipiran.com',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'http://www.fipiran.com/DataService/MFNAVIndex',
}


def search(share):
    response = requests.post('http://www.fipiran.com/DataService/AutoCompletefund', headers=headers,
                             data={'id': share.ticker})

    return response.json()


def download_nav_history():
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://www.fipiran.com',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': 'http://www.fipiran.com/DataService/MFNAVIndex',
        'Upgrade-Insecure-Requests': '1',
    }

    response = requests.post('http://www.fipiran.com/DataService/ExportMF', headers=headers,
                             data={"RegNoN": "11308", "MFStart": "1394/01/01", "MFEnd": "1399/01/01"})
    return response.text
