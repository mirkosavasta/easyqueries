# easyqueries
With this class you can query your databases using 3 lines of code. It's based on SQLAlchemy and seamlessly handles SQL Server Port Forwarding and SSH Tunnel creation for you. It converts SQL query results to pandas dataframes. 

```python
query = "SELECT * FROM table;"

with Connection("ENV_NAME") as conn:
    query_results = conn.get_dataframe(query)
```

