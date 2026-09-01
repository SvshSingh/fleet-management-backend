# check=skip=FromPlatformFlagConstDisallowed
# The pin below is deliberate - the brief states an x86_64 Linux evaluation
# environment, so the image must be amd64 even if built on Apple Silicon.
# That is exactly what this lint rule objects to, hence the skip.
FROM --platform=linux/amd64 python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

# Requirements first so the dependency layer is cached across code edits.
COPY requirements.txt .
# --root-user-action=ignore: installing as root then dropping to a non-root
# USER below is the intended shape for a container, so pip's warning about it
# is noise on a reviewer's first build.
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY app ./app

# Non-root: this container has no business writing anywhere but its data volume.
RUN useradd --create-home --uid 10001 fleet \
    && mkdir -p /var/lib/fleet \
    && chown -R fleet:fleet /var/lib/fleet /srv
USER fleet

EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
