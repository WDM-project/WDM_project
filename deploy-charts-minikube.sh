#!/usr/bin/env bash

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm uninstall redis
helm uninstall redis1
helm uninstall redis2

helm install -f helm-config/redis-helm-values.yaml redis bitnami/redis
helm install -f helm-config/redis-helm-values.yaml redis1 bitnami/redis
helm install -f helm-config/redis-helm-values.yaml redis2 bitnami/redis