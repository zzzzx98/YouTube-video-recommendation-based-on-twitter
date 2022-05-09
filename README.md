# YouTube-video-recommendation-based-on-twitter

##  Description of the repository:
This is our final project for ELEN6889, the instructor is Prof. Deepak Turaga, we designed YouTube video recommendation based on twitter.

## requirement:
pyspark

python3.7

tweepy=3.10.0


##  Example commands to execute the code:
1. Please enter your Youtube API in the scripts.
2. We designed a front-end for the project, the front-end will read the input username and store it in a txt file, and then call our back-end program to get the result and display it on the front end. Of course, you can also run the backend program directly, please modify your input in input.txt, and then execute the following command to get the recommended results of our program running:
```bash
python search.py
```
3. Run the following command to get the Top 5 topic words:
```bash
python topics.py
```

##  Demo show:

<img width="1280" alt="final" src="https://user-images.githubusercontent.com/93566978/167320703-7c33e12a-0ba9-42d3-acc2-0a18b3cf936d.png">

## reference:
[1] Deng, Zhengyu & Yan, Ming & Sang, Jitao & Xu, Changsheng. (2015). Twitter is Faster: Personalized Time-Aware Video Recommendation from Twitter to YouTube. ACM Transactions on Multimedia Computing, Communications, and Applications. 11. 1-23. 10.1145/2637285. 

[2] Zhao, Wayne X., Jiang Jing, Weng Jianshu, He Jing, Lim Ee-Peng, Yan Hongfei and Li Xiaoming. 2011. Comparing Twitter and Traditional Media Using Topic Models. In Advances in Information Retrieval: 33rd European Conference on IR Research, ECIR 2011, Dublin, Ireland, April 18-21. Proceedings. Cham: Springer. 

[3] https://github.com/minghui/Twitter-LDA
