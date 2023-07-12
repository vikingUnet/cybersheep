import requests

if __name__ == '__main__':
    
    #res = requests.get('http://35.228.87.167:8000/measure?track_id=2422c50048653f5e0df88661dfde43fd')
    #res = requests.get('http://35.228.87.167:8000/track')
    res = requests.get('https://cybersheep.ru/ping')
    
    #print(res.json()[1])
    #x = 59.91936028750001
    #x = round(x, 7)
    print(res)