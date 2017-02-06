# aoshima

## Description
This is the presto(https://prestodb.io/) cache api server.

## How to build
```
git clone https://github.com/wyukawa/aoshima.git
cd aoshima
./gradlew build
```

## How to execute
```
java -jar build/libs/aoshima-0.0.1-SNAPSHOT.jar
```
open http://localhost:8080/

## Execution example
```
curl -s -H "Content-type: application/json" -XPOST http://localhost:8080/v1/statement -d 'select * from test limit 10'
```

## Swagger
open http://localhost:8080/swagger-ui.html
