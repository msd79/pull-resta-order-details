# Docker Rebuild and Run Guide

## Prerequisites

- Docker and Docker Compose installed
- `.env` file configured with database credentials

## Quick Commands

### Rebuild and Run

```bash
# Stop existing containers, rebuild, and start
docker-compose down
docker-compose up --build
```

### Rebuild Without Cache

Use this if you've made changes to the Dockerfile or need a fresh build:

```bash
docker-compose build --no-cache
docker-compose up
```

### Run in Background (Detached Mode)

```bash
docker-compose up -d --build
```

### View Logs

```bash
# All containers
docker-compose logs -f

# Specific service
docker-compose logs -f restaorders
```

### Stop Containers

```bash
docker-compose down
```

## Troubleshooting

### Check Container Status

```bash
docker-compose ps
```

### Enter Running Container

```bash
docker-compose exec restaorders bash
```

### Check Database Connection

```bash
docker-compose exec restaorders python -c "from src.config.settings import get_config; print(get_config().database.connection_string)"
```

### Verify ODBC Drivers

```bash
docker-compose exec restaorders odbcinst -q -d
```

### View Recent Logs

```bash
docker-compose logs --tail=100 restaorders
```

## Common Issues

### Build Fails with Package Errors

If you see package-related errors, try rebuilding without cache:

```bash
docker-compose build --no-cache
```

### Database Connection Issues

1. Verify your `.env` file has correct credentials
2. Ensure SQL Server is accessible from the Docker network
3. Check firewall settings allow connections on port 1433

### Container Exits Immediately

Check logs for errors:

```bash
docker-compose logs restaorders
```
