# FeatForge 容器化执行器镜像。
# 同一镜像以不同角色启动：
#   api      定义 API + 状态页 + 在线/离线读取
#   worker   轮询作业队列的回填/重算执行器（可水平扩容多个副本）
#   all      单进程同时承担两者（演示用）
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FEATFORGE_DB=/data/featforge.db

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY featforge ./featforge
COPY scripts ./scripts

# 持久化卷：SQLite 元数据 + 物化存储（生产可替换为外部数据库）
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 默认启动一体化角色；K8s/Compose 中用 command 覆盖为 api 或 worker
ENTRYPOINT ["python", "-m", "featforge"]
CMD ["all", "--host", "0.0.0.0", "--port", "8000"]
