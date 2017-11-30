from language_detector import LanguageDetector
from corpus import CorpusHelper, CorpusModel
from pymongo import MongoClient
from itertools import tee
from os.path import join, dirname
from dotenv import load_dotenv
import os
import numpy as np
import time

def calcFeeling (texts):
    #Se analiza el contenido de los textos
    feelingVal = (cm.predict(texts, params))
    return feelingVal

def insertFeeling(valId, val):
    #Transforma el tipo de dato que retorna el analisis de sentimientos a un entero
    feelingValInt = np.int16(val).item()
    #Hace un update del resultado de sentimiento en el correspondiente Id del texto analizado
    coll.update_one({'_id':valId}, {'$set': {'feeling':feelingValInt}}, upsert=False)

def detectField(item):
    #Trada de ver si existe un campo 'full_text'. Si asi es ocupa este campo como texto a analizar
    try:
        return item['extended_tweet']['full_text']
    #Si detecta que no existe ese campo, ocupa el contenido de 'text' como texto a analizar
    except:
        return item['text']

def analyzeColl(coll):
    #Divide el generador de la Db en 2 identicos
    ids, text = tee(coll)
    #Crea un generador que solo contiene las '_id' de cada tweet
    idsIter = (item['_id'] for item in ids)
    #Crea un generador que solo contiene el texto de cada tweet
    #El texto puede venir dentro del json en distino lugar dependiendo de la logitud del tweet
    #se coupa detectField para donde esta la informacion deseada
    textIter = (detectField(item) for item in text)
    #Intenta analizar los datos.
    try:
        #Crea un generador con el analisis de sentimiento de todos los textos
        feelingVals = calcFeeling(textIter)
        #Recorre en forma paralela el iterador que contiene las '_Id' y el generador con los sentimeintos
        for valId, val in zip(idsIter, feelingVals):
            #Por cada dupla de datos, los insera en la db
            insertFeeling(valId, val)
    #En caso de error de conexion con la db, que el generador de texto no tenga informacion o otro error
    except ValueError as e:
        print(e)

if __name__ == '__main__':

    dotenv_path = join(dirname(__file__), 'Keys.env')
    load_dotenv(dotenv_path)
    #Conecta con la db
    if(os.environ.get('DB_USER') == '' and os.environ.get('DB_PASS') == ''):
        uri = os.environ.get('DB_HOST')
    else:
        uri = 'mongodb://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),os.environ.get('DB_PASS'),os.environ.get('DB_HOST'),os.environ.get('DB_NAME'))
    client = MongoClient(uri)
    db = client[os.environ.get('DB_NAME')]
    coll = db[os.environ.get('DB_COLLECTION')]
    #Crea un generador con todos los elementos de la Db que no tengan el campo 'feeling' con datos.
    #Solo trae de la db los campos '_id', 'text' y 'full_text' (contendio en 'extended_tweet').
    gen_texto=(i for i in coll.find({'feeling':{'$exists': False}},{'text':1,'extended_tweet.full_text':1}))

    #Entrena la red
    ch = CorpusHelper(language='spanish')
    ch.load()
    cm = CorpusModel(corpus=ch)
    params = cm.fit()
    print(cm.x_validation(params))

    #Trabaja sobre el generador de la Db
    print (time.asctime( time.localtime(time.time())))
    analyzeColl(gen_texto)
    print (time.asctime( time.localtime(time.time())))
