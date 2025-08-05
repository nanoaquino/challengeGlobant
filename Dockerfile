# Usar una imagen base oficial de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias para evitar reinstalaciones innecesarias
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código de la aplicación
COPY . .

# Exponer el puerto en el que correrá Flask
EXPOSE 5000

# Comando para ejecutar la aplicación
# --host=0.0.0.0 hace que sea accesible desde fuera del contenedor
CMD ["flask", "run", "--host=0.0.0.0"]