# Hackaton2-IRONMAN

La empresa Hypermobility quiere insertar en su cuadro de mandos nuevas fuentes de datos con el fin de analizarlas para mejorar sus servicios. 

Para ello, utilizamos la arquitectura que la empresa ya ha establecido, que permite la visualización en tiempo real.
Aquí tienes la arquitectura de alto nivel de la aplicación.

## Fuentes de datos 

- Estaciones Valenbici 
- Estado del tráfico http://apigobiernoabiertortod.valencia.es/apirtod/rest/datasets/estado_trafico.json
- Aparcabicis públicos http://mapas.valencia.es/lanzadera/opendata/aparcabicis/JSON
- Intensidad de vehículos (bicicletas y patinetes) http://apigobiernoabiertortod.valencia.es/apirtod/rest/datasets/intensidad_espiras.json
- Carril bici http://mapas.valencia.es/lanzadera/opendata/Tra-carril-bici/JSON

## Arquitectura

![Architecture](https://user-images.githubusercontent.com/55293318/73071262-a7e31780-3eb2-11ea-83ad-d9dab2748356.PNG) 

## Youtube

Aquí adjuntamos el enlace a un video explicativo de como se integran cada uno de los procesos interconectados en la arquitectura.

https://www.youtube.com/watch?v=6siPbdZi-Dw&feature=youtu.be

## Explicación de la arquitectura

En primer lugar, creamos los tópicos y suscripciones requeridos para cada indicador utilizando Google Cloud Platform (Pub/Sub). 

En segundo lugar, ingestamos los datos de la API del Ayuntamiento de Valencia, a través de Nifi, mediante la Template ubicada en la carpeta 'Nifi'. 

A continuación, con Beam, procesamos la información ingestada. En la carpeta 'Streaming' están disponibles los Scripts de Python necesarios. 

Por último, importando el Dashboard ubicado en la carpeta 'Kibana' podremos visualizar en tiempo real todos las fuentes de datos mencionadas anteriormente. 




