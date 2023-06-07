kubectl delete -f k8s/.
kubectl delete -f stress-test-k8s/kubernetes-config.
helm uninstall redis
helm uninstall redis1
helm uninstall redis2
helm install -f helm-config/redis-helm-values.yaml redis bitnami/redis
helm install -f helm-config/redis-helm-values.yaml redis1 bitnami/redis
helm install -f helm-config/redis-helm-values.yaml redis2 bitnami/redis
kubectl apply -f k8s/.
kubectl apply -f stress-test-k8s/kubernetes-config.
kubectl get pods