# Use a slim version of Python to keep the image size small (Best Practice)
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements first to leverage Docker's caching mechanism
# This speeds up subsequent builds if your code changes but libraries don't
COPY requirements.txt .

# Install dependencies
# --no-cache-dir keeps the image lean by not storing the installer cache
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Environment variable to ensure Python logs are sent straight to terminal
# without being buffered (Essential for real-time monitoring in Docker)
ENV PYTHONUNBUFFERED=1

# The command to run your producer
CMD ["python", "producer.py"]
