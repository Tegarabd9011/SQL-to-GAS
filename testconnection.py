import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
print("✅ Koneksi SQLEXPRESS berhasil!")
conn.close()
pause = input("Press Enter to next connection test...")
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
print("✅ Koneksi localhost berhasil!")
conn.close()
pause = input("Press Enter to exit...")
