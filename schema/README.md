## How to use schema

1. create mongodb
- here, I'm using docker to start a mongodb

```
cd schema;
docker compose up -d
```

- url: mongodb://admin:admin@localhost:27017/

2. Create collections

```
python3 create_unified_collection.py
```

3. Insert sample data

```
python3 insert_sample_data.py
```

4. Get data

```
python3 vpbank-function.py
```
