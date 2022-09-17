FROM cadic/bpd:vanilla

# Install the package
COPY dist  /workspace/dist
RUN pip install /workspace/dist/*.whl

# Workspace
WORKDIR /workspace
