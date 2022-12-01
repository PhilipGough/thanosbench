```bash
kubectl create secret generic objstore-config --from-file=objstore-config.yaml -o yaml --dry-run | kubectl apply -f -
```
```
