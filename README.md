# Setup

```
git clone git@github.com:jmileson/bq_storage.git
cd bq_storage
python3 -m venv venv
source tutorial-env/bin/activate
pip install -r requirements.txt
```

# Running
## Create a session
Run a query and construct a read session.  Persist session info to a file to simulate
"over the wire" transmission.

```
python paging.py construct session.txt
```

## Consume a session
Simulate reconstructing and consuming a read session.

```
python paging.py consume session.txt
```
