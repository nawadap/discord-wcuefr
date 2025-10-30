# === Dockerfile ===
FROM python:3.11-slim

# Dossier de travail
WORKDIR /app

# Copie le code dans l'image
COPY . /app

# Installe Python + dépendances
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir -r requirements.txt

# Affiche les logs en direct
ENV PYTHONUNBUFFERED=1

# Lance ton bot
CMD ["python", "main.py"]
