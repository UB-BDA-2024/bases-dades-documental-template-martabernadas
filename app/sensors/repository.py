from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from . import models, schemas
import json

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongoDB: MongoDBClient) -> models.Sensor:
    #Crea el sensor i l'emmagatzema a PostgreSQL
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    #Accedeix a la base de dades DB i a la col·lecció Sensors
    mongoDB.getDatabase('DB')
    collection=mongoDB.getCollection('Sensors')

    #Crea el document amb la informació del sensor
    document = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "longitude":sensor.longitude,
        "latitude":sensor.latitude,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        }
    }

    # Crea un índex per la ubicació
    collection.create_index([("location","2dsphere")])

    #Afegeix el document a mongoDB
    mongoDB.insertDocument(document)
    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    # Crea un diccionari amb les dades del sensor
    sensor_data={
        "velocity":data.velocity,
        "temperature": data.temperature,
        "humidity":data.humidity,
        "battery_level":data.battery_level,
        "last_seen":data.last_seen
    }

    # Passa les dades a JSON i les emmagatzema a Redis 
    redis.set(sensor_id, json.dumps(sensor_data))
    return data

def get_data(redis: RedisClient, sensor_id: int,sensor_name:str) -> schemas.Sensor:
    #Obté les dades del sensor de Redis
    db_sensordata = json.loads(redis.get(sensor_id))

    #Si no les troba llança una excepció
    if db_sensordata is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
        
    # Afegeix l'identificador i el nom del sensor a les dades obtingudes de Redis
    db_sensordata['id']=sensor_id
    db_sensordata['name']=sensor_name

    return db_sensordata

def delete_sensor(db: Session, sensor_id: int,mongoDB:MongoDBClient,redis:RedisClient):
    #Obté el sensor de postgreSQL
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    #Si no existeix llança una excepció
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    #Elimina el sensor de postgreSQL
    db.delete(db_sensor)
    db.commit()

    #Elimina el document de mongoDB
    mongoDB.getDatabase('DB')
    mongoDB.getCollection('Sensors')
    mongoDB.deleteDocument({"id": sensor_id})

    #Elimina la clau de redis
    redis.delete(sensor_id)

    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float,radius:float,redis:RedisClient,db:Session) -> List:
    #Accedeix a la base de dades i la col·lecció de mongoDB
    mongodb.getDatabase('DB')
    mongodb.getCollection('Sensors')

    # Crea una query per obtenir els sensors que tinguin els valors de longitud i latitud dins del radi establert
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},"longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}
    
    #Recuperem els documents que compleixin la condició 
    sensors_near = list(mongodb.getDocuments(query))
    
    #Per cada document obtigut actualitzem les seves dades
    for sensor in sensors_near:
        #Obtenim les dades de postgreSQL
        db_sensor=get_sensor(db=db,sensor_id=sensor['id'])
        #Obtenim les dades de redis
        db_data=get_data(redis=redis,sensor_id=db_sensor.id,sensor_name=db_sensor.name)
        #Les afegim al document
        sensor['velocity']=db_data['velocity']
        sensor['temperature']=db_data['temperature']
        sensor['humidity']=db_data['humidity']
        sensor['battery_level']=db_data['battery_level']
        sensor['last_seen']=db_data['last_seen']
    
    return sensors_near
