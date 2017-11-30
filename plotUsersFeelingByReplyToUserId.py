from pymongo import MongoClient
from itertools import tee
from os.path import join, dirname
from dotenv import load_dotenv
import tweepy
import pygal
import os
import sys

IDS = {}
#Lee fiche externo
def readTxtTerms():
	f = open ('plotUsersFeelingByReplyToUserId.txt','r')
	mensaje = f.read()
	mensaje=mensaje.split("\n")
	global IDS
	for i in mensaje:
		if i != '':
			#Crea una coleccion nameUser - Id
			userId = api.get_user(screen_name = i)
			IDS[i] = str(userId.id)

	f.close()

#Genera los graficos
def generateChart(pos,neg,userId,userName):
    pie_chart = pygal.Pie()
    pie_chart.title = 'User Name: ' + userName + ' Id: ' + userId
    pie_chart.add('Positivos', pos)
    pie_chart.add('Negativos', neg)
    pie_chart.render_to_file('PlotByReplyToUserId'+ userName +'.svg')

#Cuenta sentimientos
def countFeelingId(userName, userId, gen):
    countNeg = 0;
    countPos = 0;
    for item in gen:
        if item['in_reply_to_user_id_str'] == userId:
            if item['feeling'] == 1:
                countPos += 1
            else:
                countNeg += 1

    generateChart(countPos,countNeg,userId,userName)

if __name__ == '__main__':
	#Lee variables de entorno
    dotenv_path = join(dirname(__file__), 'Keys.env')
    load_dotenv(dotenv_path)

	#Se conecta a la db
    if(os.environ.get('DB_USER') == '' and os.environ.get('DB_PASS') == ''):
        uri = os.environ.get('DB_HOST')
    else:
        uri = 'mongodb://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),os.environ.get('DB_PASS'),os.environ.get('DB_HOST'),os.environ.get('DB_NAME'))
    client = MongoClient(uri)
    db = client[os.environ.get('DB_NAME')]
    coll = db[os.environ.get('DB_COLLECTION')]

	#Configura la API de Twitter
    auth = tweepy.OAuthHandler(os.environ.get('TWITTER_CONSUMER_TOKEN'), os.environ.get('TWITTER_CONSUMER_SECRET'))
    auth.set_access_token(os.environ.get('TWITTER_ACCESS_KEY'), os.environ.get('TWITTER_ACCESS_SECRET'))
	#Configura rate limit y manejo de errores de conexion
    api=tweepy.API(auth, retry_errors=set([401, 403, 404, 420, 429, 500, 502, 503, 504]))

	#Lee la lista de nombres de usuarios para graficar
    readTxtTerms()

	#Consulta por la lista de tweets que tengan el valor feeling y que sean respuesta por @ a algun otro usuario (in_reply_to_user_id_str)
    gen=(i for i in coll.find({'feeling':{'$exists': True},'in_reply_to_user_id_str':{'$ne': None}},{'feeling':1, 'in_reply_to_user_id_str':1}))
    lst = list(gen)

    for userName, userId in IDS.items():
        countFeelingId(userName, userId, lst)
