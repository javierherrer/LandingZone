import requests
import json
import os
import zipfile
from io import BytesIO

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
        
        # Ruta del archivo ZIP existente
        ruta_zip = r'C:\Users\Admin\Desktop\MASTER\Q2\BDM\LAB\data\data.zip'
        
        # Agregar los datos al archivo ZIP existente
        with zipfile.ZipFile(ruta_zip, 'a') as zipf:
            zipf.writestr('datos_barcelona.json', datos_bytes)
        
        print("Los datos se han agregado correctamente al archivo ZIP")
    else:
        print("Error al hacer la solicitud:", respuesta.status_code)

if __name__ == "__main__":
    main()
