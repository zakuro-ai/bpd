FROM cadic/bpd:vanilla

# Install the package
COPY dist  /workspace/dist
RUN pip install /workspace/dist/*.whl

# Test
COPY .env  /workspace/
COPY Makefile  /workspace/
RUN make tests

# Workspace
WORKDIR /workspace
