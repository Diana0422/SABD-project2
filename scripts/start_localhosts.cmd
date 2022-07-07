@echo off

@rem jobmanager flink
START http://localhost:8081
@rem prometheus
START http://localhost:9090
@rem grafana
START http://localhost:8000