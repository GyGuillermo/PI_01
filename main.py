from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import os
import csv
import numpy as np
import datetime
import traceback
import collections
import polars as pl


app = FastAPI()
file_path = "./data_concatenada.csv"
data = pd.read_csv('./data_concatenada.csv')
data_p = pl.read_csv('./data_concatenada.csv', has_header=True)

@app.get("/search")
def search_by_keyword(plataforma: str, keyword: str):
    try:
        if os.path.exists(file_path):
            # Se filtra el dataframe para obtener los registros que comienzan con el primer caracter del parametro plataforma
            result = data[data['id'].str.startswith(plataforma[0])]
            #filtrar el dataframe según la keyword
            filtered_df = result[result['title'].str.contains(keyword)]

            #obtener la cantidad de veces que aparece la keyword en el título
            count = len(filtered_df)
            return {"plataforma": plataforma, "keyword": keyword, "count": count}
        else:
            # El archivo no existe, maneja el error
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
  

@app.get("/movies/{plataforma}/{puntaje}/{year}")
def movie_for_platform(plataforma: str, puntaje: int, year: int):
    try:
        filtered_data = data[(data['id'].str[0] == plataforma[0])] 
        filtered_data = filtered_data[(filtered_data['score'] > puntaje)] 
        filtered_data = filtered_data[(filtered_data['release_year'] == year)]
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"La columna {e} no existe en el dataset")
    except:
        raise HTTPException(status_code=404, detail="Datos no disponibles")
    
    if len(filtered_data) == 0:
        raise HTTPException(status_code=404, detail="No se encontraron películas con los parámetros proporcionados")
    return {"cantidad de peliculas": len(filtered_data)}




#Definimos una ruta
@app.get('/movie/{plataforma}')

#Definimos una función para obtener la segunda película con el mayor score de una plataforma
def get_second_highest_movie_by_platform(plataforma: str):
    # Filtramos el dataframe para obtener los registros que comienzan con el primer caracter del parámetro plataforma
    try:
        filtered_df = data[data['id'].str.startswith(plataforma[0])]
        #Ordenamos el dataframe según la columna 'score' de forma descendente
        sorted_df = filtered_df.sort_values(by='score', ascending=False)
        #Usamos la librería polars para obtener el título y el score de la segunda película con el mayor score
        titulo = pl.find_second_highest(sorted_df['title'], sorted_df['score'])
        second_highest_score = pl.find_second_highest(sorted_df['score'], sorted_df['title'])
        #Devolvemos el título y el score de la segunda película con el mayor score
        return (plataforma,titulo,second_highest_score)
    except Exception as e:
        raise HTTPException(status_code=400, detail={"message": "Ha ocurrido un error: {}".format(e)})



@app.get("/longest_movie/{platform}/{duration_type}/{year}")
def longest_movie(platform, duration_type, year):
    try:
        caract = platform[0]
        # Se filtra el DataFrame para obtener solo las filas con el año, plataforma y tipo de duración específicos
        filtered_df = data_p.filter(pl.col("id").str.contains(caract) & (pl.col("release_year") == year) & (pl.col("duration_type") == duration_type))
        sorted_df = filtered_df.sort("duration_int", reverse=True)
        if sorted_df.count() == 0:
            raise HTTPException(status_code=404, detail="La plataforma no existe o no tiene peliculas que cumplan con los parametros especificados")
        salida = sorted_df.row(0)
        title = salida[3]
        duration = salida[15]
        return {"título": title, "duración": duration}
    except ValueError as err:
        raise HTTPException(status_code=400, detail="Error de entrada de datos: {err}")




@app.get("/pelicula/mas/larga/{plataforma}/{tipo_duracion}/{anio}")
def pelicula_mas_larga(plataforma: str, tipo_duracion: str, anio: int):
    try:
        # Se filtra el DataFrame para obtener solo las filas con el año, plataforma y tipo de duración específicos
        filtered_data = data.query("release_year == @anio and id.str[0] == @plataforma[0] and duration_type == @tipo_duracion")
        # Se ordena el DataFrame filtrado por la duración
        filtered_data = filtered_data.sort_values(by='duration_int', ascending=False)
        #Se valida si hay alguna pelicula con los criterios especificados
        if filtered_data.empty:
            return {"detail": {"error": "No se encontraron peliculas con los criterios especificados"}}
        else:
            # Se devuelve el título y la duracion de la película
            title = filtered_data.iloc[0]['title']
            duration = filtered_data.iloc[0]['duration_int']
            return {"titulo": title, "duracion": duration}
    except Exception as e:
        return {"detail": {"error": str(e)}}
