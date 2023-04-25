cat <<EOM > Dockerfile
FROM apache/airflow:2.2.4

RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql
RUN pip install --no-cache-dir pymssql
RUN pip install --no-cache-dir apache-airflow-providers-odbc
EOM

cat <<EOM > kind-cluster.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:${reg_port}"]
    endpoint = ["http://${reg_name}:${reg_port}"]
nodes:
- role: control-plane
- role: worker
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "node=worker_1"
- role: worker
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "node=worker_2"
- role: worker
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "node=worker_3"
EOM

# Install Docker
sudo apt update
sudo apt install docker.io -y

# Instal kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
curl -LO "https://dl.k8s.io/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install helm
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh

# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.18.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Create a kubernetes cluster of 1 control plane and 3 worker nodes
sudo kind create cluster --name airflow-cluster --config kind-cluster.yaml

# Add the official repository of the Airflow Helm Chart
sudo helm repo add apache-airflow https://airflow.apache.org

# Update the repo
sudo helm repo update

# Create namespace airflow
sudo kubectl create namespace airflow

# Create a key
sudo ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa

# Create a Secret
sudo kubectl create secret generic airflow-ssh-git-secret --from-file=gitSshKey=.ssh/id_rsa -n airflow

# Adicione a chave publica em Repo > Settings > Deploy keys
sudo cat .ssh/id_rsa.pub

# Create Dockerfile
# Create Image
sudo docker build -t airflow_custom:1.0.0 .

# Load image into cluster
sudo kind load docker-image airflow_custom:1.0.0 --name airflow-cluster

sudo helm install airflow apache-airflow/airflow -n airflow \
--set images.airflow.repository=airflow_custom \
--set images.airflow.tag=1.0.0 \
--set dags.gitSync.enabled=true \
--set dags.gitSync.repo=git@github.com:belmino15/antaq_data_lake.git \
--set dags.gitSync.branch=master \
--set dags.gitSync.depth=0 \
--set dags.gitSync.subPath="dags" \
--set dags.gitSync.sshKeySecret=airflow-ssh-git-secret \
--debug

sudo cat .ssh/id_rsa.pub
sudo kubectl port-forward svc/airflow-webserver --address 0.0.0.0 8080:8080 -n airflow --context kind-airflow-cluster
