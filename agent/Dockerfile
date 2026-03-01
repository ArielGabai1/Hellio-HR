# Install Gmail MCP server
FROM node:18-alpine AS gmail-mcp
RUN npm install -g @gongrzhe/server-gmail-autoauth-mcp

# Agent runtime
FROM python:3.14.2-alpine

# Install Node.js runtime (needed to run the Gmail MCP)
RUN apk add --no-cache nodejs

# Copy Gmail MCP 
COPY --from=gmail-mcp /usr/local/lib/node_modules/@gongrzhe/server-gmail-autoauth-mcp /opt/gmail-mcp

# Install Python deps
RUN apk add --no-cache gcc musl-dev
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
WORKDIR /app

RUN adduser -D -H agent \
    && mkdir -p /tmp/downloads \
    && chown agent /tmp/downloads
USER agent

CMD ["python", "agent.py"]
