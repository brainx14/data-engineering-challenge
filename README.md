# Data Engineering Challenge - Globant

Solución al challenge de ingeniería de datos de Globant. Procesamiento realizado en PySpark, con análisis de contrataciones por trimestre y reporte de departamentos destacados.

## ¿Por qué PySpark?

La solución fue desarrollada en PySpark, ya que es un framework ampliamente compatible con servicios en la nube, como AWS Glue. Esto permite escalar la arquitectura fácilmente y migrar a un entorno **serverless** y distribuido si el volumen de datos lo requiere.

## Arquitectura sugerida para la nube (AWS Data Lake)

- **S3**: Almacenamiento centralizado de los archivos de datos (Data Lake).
- **AWS Glue**: Procesamiento de datos en PySpark (ETL/ELT serverless).
- **Step Functions**: Orquestación de los procesos de ingesta, transformación y carga.
- **Athena**: Consulta SQL sobre los datos almacenados en S3 de manera serverless.
- **AWS Glue Data Catalog**: Gobierno y catalogación de los datasets.
- **IAM**: Control de accesos y permisos a nivel granular para cada servicio y usuario.
- **CodeCommit / CodePipeline / CloudFormation**: Integración y despliegue continuo (CI/CD), así como infraestructura como código para mantener y versionar la arquitectura.

Esta arquitectura permite mantener los datos centralizados, seguros, trazables y siempre listos para análisis en batch, maximizando la flexibilidad y escalabilidad del sistema.
