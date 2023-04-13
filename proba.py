import sqlite3

# conectarse a la base de datos
conn = sqlite3.connect('./register_uploads/register_uploads.db')

# crear un cursor
c = conn.cursor()

# crear una tabla
# c.execute('''CREATE TABLE stocks
#              (date text, trans text, symbol text, qty real, price real)''')

# ejecutar una consulta SELECT
c.execute("SELECT * FROM log")

# recuperar los datos
data = c.fetchall()

# imprimir los datos
print(data)



# guardar los cambios
conn.commit()

# cerrar la conexión
conn.close()