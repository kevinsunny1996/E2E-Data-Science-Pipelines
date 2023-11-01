from google.cloud import secretmanager

class CloudUtils():
  def __init__(self, project_num):
    self.client = secretmanager.SecretManagerServiceClient()
    self.project_num = project_num

  def get_secret(self, secret_id) -> str:
    secret_path = f"projects/{self.project_num}/secrets/{secret_id}/versions/latest"
    response = self.client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")
  
  def get_workspace():
    pass