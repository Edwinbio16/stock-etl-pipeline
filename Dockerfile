# Start from an official slim Python image. "slim" = Debian with Python and not
# much else, so the image stays small.
FROM python:3.12-slim

# Everything after this runs inside /app in the container.
WORKDIR /app

# Copy requirements FIRST, on its own, before the rest of the code.
# Docker caches each step; because requirements.txt changes rarely, this pip
# install layer gets reused on rebuilds instead of reinstalling every package
# every time you tweak a line of Python. If we copied everything at once, any
# code change would bust the cache and force a full reinstall.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the application code.
COPY . .

# Default command when the container starts: create the table, then run the flow.
CMD ["sh", "-c", "python setup_db.py && python prefect_pipeline.py"]
