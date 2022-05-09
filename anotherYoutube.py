from youtube_api import YouTubeDataAPI
import re
api_key = '' ##youtube API
yt = YouTubeDataAPI(api_key)


f3 = open('input.txt')
name1 = str(f3.read())
result = yt.search(name1)
#print(type(result))
#print(result)
#print(result[1]['channel_title'])
#print(len(result))
title = []
for i in range(len(result)):
    #print(result[i]['video_title'])
    title.append(result[i]['video_title'])

#print(title)

name2 = ""
tmp = []
for indexes in title:
    name2 += indexes
    name2 += '\n'
print(name2)

f4 = open("result2.txt","w", encoding='utf-8')
f4.write(name2)

f3.close()
f4.close()