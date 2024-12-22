Run docker compopse

Build container:
DB_PASSWORD=qwerty DB_PASSPHRASE=X7h3NkL9qzVm docker-compose up --build

python3 -c "import pyodbc; print(pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.11\\SQLEXPRESS;Database=RestaOrders1;UID=sa;PWD=qwerty'))"
