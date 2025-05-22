# app/Dockerfile
FROM python:3.9-slim
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY script.py .
# Adicionamos um sleep aqui para dar tempo ao postgres de iniciar completamente antes do script tentar conectar
# Em um cenario real, usariamos um script de espera mais robusto (wait-for-it.sh ou similar)
CMD ["sh", "-c", "sleep 10 && python ./script.py"]