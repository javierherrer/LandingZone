import requests
import json
import os

def main():
    # URL de la API
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=af79b704-ff47-48f7-a66b-8b9fd738a5f1&limit=877"
    
    # Realizar la solicitud GET
    respuesta = requests.get(url)
    
    # Verificar si la solicitud fue exitosa
    if respuesta.status_code == 200:
        # Convertir la respuesta a JSON
        datos_json = respuesta.json()
        
        # Convertir los datos JSON a bytes
        datos_bytes = json.dumps(datos_json).encode('utf-8')
        
        # Crear una carpeta si no existe
        carpeta_destino = '../resources/unemployment-data'
        
        if not os.path.exists(carpeta_destino):
              os.makedirs(carpeta_destino)
        
        # Escribir los datos en un archivo en la carpeta
        ruta_archivo = os.path.join(carpeta_destino, 'datos_barcelona.json')
        with open(ruta_archivo, 'wb') as archivo:
            archivo.write(datos_bytes)
        
        print("Los datos se han guardado correctamente en la carpeta:", carpeta_destino)
    else:
        print("Error al hacer la solicitud:", respuesta.status_code)

if __name__ == "__main__":
    main()
