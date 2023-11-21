# Seguridad_Social_Salesforce
Conexión de una base de datos con un objeto de una org de Salesforce

Tercera parte de mi pequeño proyecto de bases de datos de pensionistas, ahora conectamos nuestra base de datos con el objeto custom pensionista__c de una org de Salesforce. Creamos los campos en el objeto pensionista__c a imagen y semejanza de nuestra base de datos y a través de una api hecha en python realizamos la importación de los registros. Los registros con el nº de identificación ya existente(nuestro código hash que simulaun DNI único) se actualizan en nuestra org y los nuevos se insertan