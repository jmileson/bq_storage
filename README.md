# Setup

```
git clone git@github.com:jmileson/bq_storage.git
cd bq_storage
python3 -m venv venv
source env/bin/activate
pip install -r requirements.txt
```

# Running
## Create a session
Run a query and construct a read session.  Persist session info to a file to simulate
"over the wire" transmission.

```
python paging.py construct data/session.txt
```

You can also configure the number of streams with 

```
python paging.py construct data/session.txt -n 10
```

Be warned that Google doesn't respect this number if they determine that the number of
streams won't improve throughput.

## Consume a session
Simulate reconstructing and consuming a read session.

```
python paging.py consume data/session.txt
```
