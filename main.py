# para MongoDB:
import pymongo
import pandas as pd
import os
import time
from datetime import datetime
import mplfinance as mpf
import requests
import numpy as np
import time

factor = os.environ["FACTOR"]
factor = float(factor)
def telegram_bot_sendtext(mensaje):
  bot_mensaje = mensaje
  bot_token = "1893390873:AAE63gdnXuWbP4vCaMBDt-L-nj1-ow4pMAI"
  bot_chatID = '-575404991' #para grupos, el chat ID es el que figura en la url en navegador, A VECES HACE FALTA agregando un "100" entre el signo negativo y primer digito.
  send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + \
              '&parse_mode=MarkdownV2&text=' + bot_mensaje
  response = requests.get(send_text)
  return response.json()

def telegram_bot_sendPhoto(filename): #Función para enviar imagen a telegram
  bot_filename = filename
  bot_token = "1893390873:AAE63gdnXuWbP4vCaMBDt-L-nj1-ow4pMAI"
  bot_chatID = '-575404991' #para grupos, el chat ID es el que figura en la url en navegador, A VECES HACE FALTA agregando un "100" entre el signo negativo y primer digito.
  files = {'photo':open(filename, mode ='rb')}
  send_ph = 'https://api.telegram.org/bot' + bot_token + '/sendPhoto?chat_id=' + bot_chatID
  response = requests.post(send_ph, files= files)
  return response.json()

def getUltimoFechaString(ultimoTimestamp):
  ultimoFecha = datetime.fromtimestamp(ultimoTimestamp/1000)
  #ultimoFecha = ultimoHs.isoformat()
  #ultimoFecha = ultimoHs.ctime()
  ultimoMes= ultimoFecha.month
  ultimoDia= ultimoFecha.day
  ultimoHora= ultimoFecha.hour -3 #se resta 3hs para pasar de horario UTC a horario Arg.
  if ultimoHora < 0:
    ultimoHora = ultimoHora +24
    ultimoDia = ultimoDia -1
  ultimoMinuto = ultimoFecha.minute

  ultimoFechaString = f"{ultimoDia}/{ultimoMes} a las {ultimoHora}:{ultimoMinuto}hs"
  return ultimoFechaString  

def crear_imagen (df):
  grafica=pd.DataFrame()
  grafica['Open']   = df['o']
  grafica['High']   = df['h']
  grafica['Low']    = df['l']
  grafica['Close']  = df['c']
  grafica['Volume'] = df['v']
  grafica['SMA_Vol'] = df['SMA_Vol']
  grafica["mano negra"] = df["mano negra"]
  grafica["markers"] = df["markers"] #Es importante que no sean todos los numeros nan si no tira error
  grafica["markersPrice"] = df["markersPrice"] #Es importante que no sean todos los numeros nan si no tira error
  grafica['Date'] = pd.to_datetime(df['t'], unit='ms')
  grafica.set_index('Date', inplace=True, drop=True)
  apds = [mpf.make_addplot(grafica['SMA_Vol'],panel = "lower"), mpf.make_addplot(grafica.markers,scatter=True, marker= "v",panel = "lower"), mpf.make_addplot(grafica.markersPrice,scatter=True, marker= "v")]
  filename = "btc"+".jpg" # el filename en el futuro dependera de la moneda que estemos mirando, en este caso, definimos btc arbitrariamente
  mpf.plot(grafica, type='candle',figratio = (50,20),mav=(20,40), style='binance', volume=True, addplot = apds, savefig = filename)
  return filename

# Se configura conexión a base de datos en Mongo DB Atlas:

User = "usuario2"
Pass = "Cryptonlokita1"
clustername = "Cluster0"

query = f"mongodb+srv://{User}:{Pass}@{clustername}.qcekm.mongodb.net"
client = pymongo.MongoClient(query)


# Set database name to work with. If it doesn't exist, it will be created as soon as one document is added.

db = client.websocket #"websocket" es el nombre de nuestra database en Mongo DB
collection = db.candles  #"candles" es el nombre de nuestra colección en la database "websocket"

#Se traen los últimos 40 datos desde database yse cargan en un dataframe
ventana = 40
data = list (collection.find({"s":"BTCUSDT"}).skip(collection.count() - ventana))
df = pd.DataFrame(data)

#SE AGREGAN LAS COLUMNAS ADICIONALES QUE HARÁN FALTA PARA LAS ALERTAS DE VOLUMEN BALLENA

df["SMA_Vol"] = np.nan #df.v.astype(float).rolling(ventana).mean()
df["Mano Negra"] = np.nan
df["Markers"] = np.nan
df["Markers Price"] = np.nan

fechaAnterior = 0

#Bucle infinito: CONSULTA Y DETECCIÓN DE DATO NUEVO EN DATABASE Y EVALUACIÓN DE CONDICIONES PARA ALERTA. Obtengo un flag "hayNuevo" en True.

#DETECCION DE DATO NUEVO. (Esto seguramene se puede optimizar para detectar el dato nuevo en vez de consultar y comparar fecha)
while True:
  
  ultimoDocumento = list(collection.find().skip(collection.count() - 1)) # Busco el último Documento dentro de la Colección en MongoDB
  fechaUltima = ultimoDocumento[0]["t"] # Del último documento (que es una lista con un solo elemento de la colección) tomo t que es la fecha

  if  fechaUltima > fechaAnterior:
    fechaAnterior = fechaUltima
    hayNuevo = True

  else:
    hayNuevo =False
    time.sleep(20) # ACA FALTA PARAMETRIZAR EL VALOR DE SLEEP

  
  if hayNuevo:
    print("Hay nuevo")

    #SE AGREGA ULTIMO DOCUMENTO AL DATAFRAME Y SE CALCULA SU VALOR DE SMA_VOL PARA FUTUROS USOS
    df = df.append(ultimoDocumento, ignore_index=True)

    #df["SMA_Vol"][40] = df.v.mean() # no hacemos rolling ya que la ventana se la estamos dando nosotros, calculamos el promedio total y se lo asignamos al ultimo valor
    df.loc[ventana,"SMA_Vol"] = df["v"].mean()
        
    #SE BORRA PRIMERA FILA PARA MANTENER DATAFRAME DE 40 ELEMENTOS
    df.drop(axis = 0, index = df.index[0], inplace=True)

    #SE RESETEAN LOS INDEX PARA QUE SEAN DE 0 A 39
    df = df.reset_index(drop=True)
        
    #SE VERIFICA SI SE CUMPLE LA CONDICION DE VOLUMEN BALLENA

    ultimoVol = df.loc[ventana-1,"v"] # toma última fila, columna 11 es la columna "v", o sea volumen
    SMA_Vol = df.loc[ventana-2,"SMA_Vol"] #toma anteúltima fila, columna 18 es la SMA_Vol calculada
    ultimoFecha = df.loc[ventana-1,"t"] #fecha y hora del último dato, formato timestamp
    print("ultimoVol es " + str(ultimoVol) + " y SMA_Vol es " + str(SMA_Vol))

    ultimoFechaString = getUltimoFechaString(ultimoFecha)
    ultimoFechaString
    
    
    if ultimoVol > factor*SMA_Vol:
      print("HAY BALLENA")
      df["mano negra"] =   np.where(df.v>factor*df.SMA_Vol, "mano negra", "pobres")
      df["SMA_Vol"] =      np.where(df["SMA_Vol"]=="nan", np.nan, df["SMA_Vol"])
      df["markers"] =      np.where(df["mano negra"]=="mano negra", df.v*1.0001, np.nan)
      df["markersPrice"] = np.where(df["mano negra"]=="mano negra", df.c*1.0002, np.nan)
      ultimoVolstr = str(ultimoVol.round(1)).replace(".",",")
      factorstr = str(factor).replace(".",",")
      mensaje = f"Alerta volumen ballena BTCUSDT temporalidad 1 minuto, volumen {ultimoVolstr} BTC mayor a {factorstr} veces la media , fecha: {ultimoFechaString}"
      print(df.loc[:,"v":"mano negra"].tail())
      telegram_bot_sendtext(mensaje)   
      filename = crear_imagen(df)      # Creo imagen
      telegram_bot_sendPhoto(filename) # Envío la imagen a Telegram
      
    
    time.sleep(20) # ACA FALTA PARAMETRIZAR EL VALOR DE SLEEP

    
