# Log in
az login --use-device-code

# Create Resource Group and VM
az group create --name myResourceGroup --location westeurope
az vm create \
  --resource-group myResourceGroup \
  --name airflow-vm \
  --image Ubuntu2204 \
  --admin-username azureuser \
  --generate-ssh-keys

# Open required ports
for port in 22 8080 8081 3000; do
  az vm open-port --port $port --resource-group myResourceGroup --name airflow-vm
done

# Get public IP
PUBLIC_IP=$(az vm show --name airflow-vm --resource-group myResourceGroup -d --query publicIps -o tsv)
echo "Public IP is: $PUBLIC_IP"

# SCP and SSH
scp -r . azureuser@$PUBLIC_IP:/home/azureuser/
ssh azureuser@$PUBLIC_IP
