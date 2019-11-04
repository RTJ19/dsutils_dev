from google.colab import drive

def mount_drive(folder_name):
  # This will prompt for authorization.
  drive.mount('/content/drive')
  file_path = "/content/drive/My Drive/"+ folder_name +"/"
  return file_path
