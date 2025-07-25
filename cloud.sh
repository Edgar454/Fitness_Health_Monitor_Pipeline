# Log in
az login --use-device-code

# Create Resource Group and VM
az group create --name myResourceGroup --location westeurope
az vm create \
  --resource-group myResourceGroup \
  --name airflow-vm \
  --image Ubuntu2204 \
  --admin-username azureuser \
  --size Standard_DS2_v2 \
  --generate-ssh-keys

# Open required ports
START_PRIORITY=1300
RESOURCE_GROUP="myResourceGroup"
VM_NAME="airflow-vm"

PORTS=(22 8080 8081 3000)

for i in "${!PORTS[@]}"; do
  PORT=${PORTS[$i]}
  PRIORITY=$((START_PRIORITY + i))
  echo "Opening port $PORT with priority $PRIORITY..."
  az vm open-port \
    --port $PORT \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VM_NAME" \
    --priority $PRIORITY
done


# Get public IP
PUBLIC_IP=$(az vm show --name airflow-vm --resource-group myResourceGroup -d --query publicIps -o tsv)
echo "Public IP is: $PUBLIC_IP"

# SCP and SSH
scp -r . azureuser@$PUBLIC_IP:/home/azureuser/
ssh azureuser@$PUBLIC_IP
