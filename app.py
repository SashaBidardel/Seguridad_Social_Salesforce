from flask import Flask, jsonify
from simple_salesforce import Salesforce, SalesforceMalformedRequest
from sqlalchemy import create_engine, text
from datetime import datetime
from config import sf_usuario, sf_contraseña, sf_token
from config import mysql_host1, mysql_usuario1, mysql_contraseña1, mysql_base_de_datos1
app = Flask(__name__)

# Configuración de la conexión Salesforce
sf_usuario = sf_usuario
sf_contraseña = sf_contraseña
sf_token = sf_token

# Crear conexión Salesforce
sf = Salesforce(username=sf_usuario, password=sf_contraseña, security_token=sf_token)

@app.route('/exportar-datos', methods=['GET'])
def exportar_datos():
    try:
        # Configuración de la conexión MySQL
        mysql_host = mysql_host1
        mysql_usuario = mysql_usuario1
        mysql_contraseña = mysql_contraseña1
        mysql_base_de_datos = mysql_base_de_datos1

        # Crear conexión MySQL
        conexion_mysql = create_engine(f'mysql+mysqlconnector://{mysql_usuario}:{mysql_contraseña}@{mysql_host}/{mysql_base_de_datos}')

        # Consulta MySQL
        query_mysql = """
            SELECT Nombre, Apellidos, Numero_Identificacion, Fecha_de_Nacimiento, Fecha_de_Jubilacion,
            Bases_de_Cotizacion, Comunidad_Autonoma, Invalidez, Base_Reguladora, Total_Meses_Cotizados,
            Pension, Pension_Final, Anos_Enteros_Cotizados, Meses_Cotizados, Anos_Cotizados_Decimal,
            Edad, Grupo_Edades, Pension_Final_con_Complementos, Vivo, Correo, Bueno
            FROM datos
        """

        with conexion_mysql.connect() as connection:
            resultados_mysql = connection.execute(text(query_mysql))

            datos_a_exportar = []
            registros_a_insertar = []
            registros_a_actualizar = []

            for row in resultados_mysql:
                data = {
                    'Name': row[0],
                    'Apellidos__c': row[1],
                    'Numero_Identificacion__c': row[2],
                    'Fecha_de_Nacimiento__c': datetime.strftime(row[3], "%Y-%m-%d") if row[3] else None,
                    'Fecha_de_Jubilacion__c': datetime.strftime(row[4], "%Y-%m-%d") if row[4] else None,
                    'Bases_de_Cotizacion__c': float(row[5]),
                    'Comunidad_Autonoma__c': row[6],
                    'Invalidez__c': bool(row[7]),
                    'Base_Reguladora__c': float(row[8]),
                    'Total_Meses_Cotizados__c': int(row[9]),
                    'Pension__c': float(row[10]),
                    'Pension_Final__c': float(row[11]),
                    'Anos_Enteros_Cotizados__c': int(row[12]),
                    'Meses_Cotizados__c': int(row[13]),
                    'Anos_Cotizados_Decimal__c': float(row[14]),
                    'Edad__c': int(row[15]),
                    'Grupo_Edades__c': row[16],
                    'Pension_Final_con_Complementos__c': float(row[17]),
                    'vivo__c': bool(row[18]),
                    'correo__c': row[19],
                    'bueno__c': bool(row[20])
                }
                datos_a_exportar.append(data)

        if datos_a_exportar:
            for data in datos_a_exportar:
                numero_identificacion = data['Numero_Identificacion__c']
                existing_records = sf.query(f"SELECT Id, Numero_Identificacion__c FROM Pensionista__c WHERE Numero_Identificacion__c = '{numero_identificacion}'")
                if existing_records['records']:
                    data['Id'] = existing_records['records'][0]['Id']
                    registros_a_actualizar.append(data)
                else:
                    registros_a_insertar.append(data)

            if registros_a_insertar:
                registros_creados = sf.bulk.Pensionista__c.insert(registros_a_insertar)
                for registro in registros_creados:
                    if registro['success']:
                        print(f"Registro insertado en Salesforce: {registro['id']}")
                    else:
                        print(f"Error al insertar el registro: {registro['errors']}")

            if registros_a_actualizar:
                for registro in registros_a_actualizar:
                    record_id = registro['Id']
                    del registro['Id']
                    sf.Pensionista__c.update(record_id, registro)
                    print(f"Registro actualizado en Salesforce: {record_id}")

            return jsonify({"message": "Datos exportados a Salesforce correctamente"}), 200
        else:
            return jsonify({"error": "No se encontraron datos para exportar"}), 404

    except SalesforceMalformedRequest as e:
        error_message = f"Error en la solicitud a Salesforce: {str(e)}"
        print(error_message)
        print(e.content)
        return jsonify({"error": error_message}), 500

    except Exception as e:
        error_message = f"Error inesperado: {str(e)}"
        print(error_message)
        return jsonify({"error": error_message}), 500

if __name__ == '__main__':
    app.run(port=5002, debug=True)


