# check=skip=FromPlatformFlagConstDisallowed
# The pin below is deliberate - the brief states an x86_64 Linux evaluation
# environment, so the image must be amd64 even if built on Apple Silicon.
# That is exactly what this lint rule objects to, hence the skip.
FROM --platform=linux/amd64 python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt .
# --root-user-action=ignore: installing as root then dropping to a non-root
# USER below is the intended shape for a container, so pip's warning about it
# is noise on a reviewer's first build.
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY supervisor.py robot_publisher.py ./

RUN useradd --create-home --uid 10002 robots
USER robots

# The supervisor IS the entrypoint: `docker compose up` starts the simulation with no
# manual step in a second terminal, as the brief requires.
CMD ["python", "-u", "supervisor.py"]
